"""Multi-source ETL pipeline with data-quality validation, reconciliation and generated lineage docs.

Flow:  extract (raw files -> staging)  ->  validate (SQL rules -> quarantine)
       ->  load (idempotent upserts into a star schema)  ->  reconcile  ->  document
"""
import csv
import json
import logging
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("pipeline")

# ----------------------------------------------------------------------------- schema
STAGING_DDL = """
DROP TABLE IF EXISTS stg_orders;    DROP TABLE IF EXISTS stg_customers;  DROP TABLE IF EXISTS stg_products;
CREATE TABLE stg_orders    (src_row INTEGER, order_id TEXT, order_date TEXT, customer_id TEXT, material_id TEXT,
                            quantity TEXT, net_value TEXT, currency TEXT);
CREATE TABLE stg_customers (src_row INTEGER, customer_id TEXT, name TEXT, region TEXT, segment TEXT);
CREATE TABLE stg_products  (src_row INTEGER, material_id TEXT, description TEXT, category TEXT, unit_price TEXT);
"""

WAREHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS dq_rejects   (run_id TEXT, source TEXT, src_row INTEGER, rule TEXT, severity TEXT);
CREATE TABLE IF NOT EXISTS recon_results(run_id TEXT, check_name TEXT, expected REAL, actual REAL, status TEXT);
CREATE TABLE IF NOT EXISTS dim_customer (customer_key INTEGER PRIMARY KEY AUTOINCREMENT, customer_id TEXT UNIQUE,
                                         name TEXT, region TEXT, segment TEXT, run_id TEXT);
CREATE TABLE IF NOT EXISTS dim_product  (product_key INTEGER PRIMARY KEY AUTOINCREMENT, material_id TEXT UNIQUE,
                                         description TEXT, category TEXT, unit_price REAL, run_id TEXT);
CREATE TABLE IF NOT EXISTS fact_sales_orders (order_id TEXT PRIMARY KEY, order_date TEXT NOT NULL,
                                         customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
                                         product_key  INTEGER NOT NULL REFERENCES dim_product(product_key),
                                         quantity INTEGER, net_value REAL, currency TEXT, run_id TEXT);
CREATE VIEW IF NOT EXISTS v_monthly_revenue_by_region AS
  SELECT strftime('%Y-%m', f.order_date) AS month, c.region, COUNT(*) AS orders, ROUND(SUM(f.net_value), 2) AS revenue
  FROM fact_sales_orders f JOIN dim_customer c USING (customer_key) GROUP BY 1, 2;
"""

# ----------------------------------------------------------------------------- data-quality rules
# (source, rule, severity, SQL predicate that is TRUE when the staged row FAILS the rule)
BLANK = "IS NULL OR TRIM({c}) = ''"
RULES = [
    ("orders", "ORD_MISSING_KEY",      "reject", f"order_id {BLANK.format(c='order_id')}"),
    ("orders", "ORD_MISSING_CUSTOMER", "reject", f"customer_id {BLANK.format(c='customer_id')}"),
    ("orders", "ORD_INVALID_DATE",     "reject", "date(order_date) IS NULL"),
    ("orders", "ORD_INVALID_QUANTITY", "reject", "quantity IS NULL OR CAST(quantity AS REAL) <= 0"),
    ("orders", "ORD_NEGATIVE_VALUE",   "reject", "net_value IS NULL OR CAST(net_value AS REAL) < 0"),
    ("orders", "ORD_UNKNOWN_PRODUCT",  "reject", "material_id NOT IN (SELECT material_id FROM stg_products)"),
    ("orders", "ORD_UNKNOWN_CUSTOMER", "reject",
     "customer_id IS NOT NULL AND TRIM(customer_id) <> '' AND customer_id NOT IN (SELECT customer_id FROM stg_customers)"),
    ("orders", "ORD_DUPLICATE",        "reject", "src_row NOT IN (SELECT MIN(src_row) FROM stg_orders GROUP BY order_id)"),
    ("customers", "CUS_MISSING_KEY",   "reject", f"customer_id {BLANK.format(c='customer_id')}"),
    ("customers", "CUS_DUPLICATE",     "reject", "src_row NOT IN (SELECT MIN(src_row) FROM stg_customers GROUP BY customer_id)"),
    ("customers", "CUS_MISSING_REGION", "warn",  f"region {BLANK.format(c='region')}"),
    ("products", "PRD_MISSING_KEY",    "reject", f"material_id {BLANK.format(c='material_id')}"),
    ("products", "PRD_INVALID_PRICE",  "reject", "unit_price IS NULL OR TRIM(unit_price) = '' OR CAST(unit_price AS REAL) <= 0"),
]
STG = {"orders": "stg_orders", "customers": "stg_customers", "products": "stg_products"}

# source field -> target column lineage (used to generate documentation)
LINEAGE = [
    ("fact_sales_orders.order_id",     "sap_sales_orders.csv: order_id",    "Primary key; first occurrence kept, duplicates quarantined"),
    ("fact_sales_orders.order_date",   "sap_sales_orders.csv: order_date", "Validated as ISO date"),
    ("fact_sales_orders.customer_key", "sap_sales_orders.csv: customer_id","Looked up in dim_customer (surrogate key)"),
    ("fact_sales_orders.product_key",  "sap_sales_orders.csv: material_id","Looked up in dim_product (surrogate key)"),
    ("fact_sales_orders.quantity",     "sap_sales_orders.csv: quantity",   "Cast to integer; must be > 0"),
    ("fact_sales_orders.net_value",    "sap_sales_orders.csv: net_value",  "Cast to decimal; must be >= 0"),
    ("dim_customer.region",            "crm_customers.json: region",       "Missing values defaulted to 'Unknown' (warning logged)"),
    ("dim_customer.segment",           "crm_customers.json: segment",      "Loaded as-is"),
    ("dim_product.unit_price",         "product_master.csv: unit_price",   "Cast to decimal; must be > 0"),
    ("dim_product.category",           "product_master.csv: category",     "Loaded as-is"),
]


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ----------------------------------------------------------------------------- extract
def extract(conn, raw_dir):
    """Load the three source files into transient staging tables. Returns source row counts."""
    raw = Path(raw_dir)
    conn.executescript(STAGING_DDL)
    counts = {}
    with open(raw / "sap_sales_orders.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    conn.executemany("INSERT INTO stg_orders VALUES (?,?,?,?,?,?,?,?)",
                     [(i, r["order_id"], r["order_date"], r["customer_id"], r["material_id"],
                       r["quantity"], r["net_value"], r["currency"]) for i, r in enumerate(rows, 1)])
    counts["orders"] = len(rows)
    with open(raw / "crm_customers.json") as f:
        rows = json.load(f)
    conn.executemany("INSERT INTO stg_customers VALUES (?,?,?,?,?)",
                     [(i, r.get("customer_id"), r.get("name"), r.get("region"), r.get("segment"))
                      for i, r in enumerate(rows, 1)])
    counts["customers"] = len(rows)
    with open(raw / "product_master.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    conn.executemany("INSERT INTO stg_products VALUES (?,?,?,?,?)",
                     [(i, r["material_id"], r["description"], r["category"], r["unit_price"])
                      for i, r in enumerate(rows, 1)])
    counts["products"] = len(rows)
    return counts


# ----------------------------------------------------------------------------- validate
def validate(conn, run_id):
    """Apply every rule; failing rows are written to dq_rejects (quarantine) rather than silently dropped."""
    for source, rule, severity, predicate in RULES:
        cur = conn.execute(
            f"INSERT INTO dq_rejects SELECT ?, ?, src_row, ?, ? FROM {STG[source]} WHERE {predicate}",
            (run_id, source, rule, severity))
        if cur.rowcount:
            log.warning("%-22s %-7s %d row(s)", rule, severity, cur.rowcount)


def _valid(source, run_id):
    return (f"SELECT * FROM {STG[source]} WHERE src_row NOT IN "
            f"(SELECT src_row FROM dq_rejects WHERE run_id = '{run_id}' AND source = '{source}' AND severity = 'reject')")


# ----------------------------------------------------------------------------- load
def load(conn, run_id):
    """Idempotent upserts: re-running the pipeline never creates duplicates."""
    conn.execute(f"""INSERT INTO dim_customer (customer_id, name, region, segment, run_id)
        SELECT customer_id, name, COALESCE(NULLIF(TRIM(region), ''), 'Unknown'), segment, '{run_id}' FROM ({_valid('customers', run_id)}) WHERE true
        ON CONFLICT(customer_id) DO UPDATE SET name=excluded.name, region=excluded.region,
                                               segment=excluded.segment, run_id=excluded.run_id""")
    conn.execute(f"""INSERT INTO dim_product (material_id, description, category, unit_price, run_id)
        SELECT material_id, description, category, CAST(unit_price AS REAL), '{run_id}' FROM ({_valid('products', run_id)}) WHERE true
        ON CONFLICT(material_id) DO UPDATE SET description=excluded.description, category=excluded.category,
                                               unit_price=excluded.unit_price, run_id=excluded.run_id""")
    conn.execute(f"""INSERT INTO fact_sales_orders
        SELECT o.order_id, date(o.order_date), c.customer_key, p.product_key,
               CAST(o.quantity AS INTEGER), CAST(o.net_value AS REAL), o.currency, '{run_id}'
        FROM ({_valid('orders', run_id)}) o
        JOIN dim_customer c ON c.customer_id = o.customer_id
        JOIN dim_product  p ON p.material_id = o.material_id WHERE true
        ON CONFLICT(order_id) DO UPDATE SET order_date=excluded.order_date, customer_key=excluded.customer_key,
             product_key=excluded.product_key, quantity=excluded.quantity, net_value=excluded.net_value,
             currency=excluded.currency, run_id=excluded.run_id""")


# ----------------------------------------------------------------------------- reconcile
def reconcile(conn, run_id, source_counts):
    """Independent source-to-target checks. Returns True only if every check passes."""
    def one(sql):
        return conn.execute(sql).fetchone()[0] or 0
    rej = lambda s: one(f"SELECT COUNT(DISTINCT src_row) FROM dq_rejects WHERE run_id='{run_id}' AND source='{s}' AND severity='reject'")
    loaded = {
        "orders": one(f"SELECT COUNT(*) FROM fact_sales_orders WHERE run_id='{run_id}'"),
        "customers": one(f"SELECT COUNT(*) FROM dim_customer WHERE run_id='{run_id}'"),
        "products": one(f"SELECT COUNT(*) FROM dim_product WHERE run_id='{run_id}'"),
    }
    checks = []
    for s in ("orders", "customers", "products"):
        staged = one(f"SELECT COUNT(*) FROM {STG[s]}")
        checks.append((f"{s}: source file rows = staged rows", source_counts[s], staged))
        checks.append((f"{s}: staged rows = loaded + rejected", staged, loaded[s] + rej(s)))
    checks.append(("orders: valid net_value total = warehouse total",
                   one(f"SELECT ROUND(SUM(CAST(net_value AS REAL)), 2) FROM ({_valid('orders', run_id)}) "
                       f"WHERE material_id IN (SELECT material_id FROM dim_product) AND customer_id IN (SELECT customer_id FROM dim_customer)"),
                   one(f"SELECT ROUND(SUM(net_value), 2) FROM fact_sales_orders WHERE run_id='{run_id}'")))
    checks.append(("fact: orphan foreign keys (expect 0)", 0,
                   one("SELECT COUNT(*) FROM fact_sales_orders f LEFT JOIN dim_customer c USING(customer_key) "
                       "LEFT JOIN dim_product p USING(product_key) WHERE c.customer_key IS NULL OR p.product_key IS NULL")))
    ok = True
    for name, expected, actual in checks:
        status = "PASS" if abs(float(expected) - float(actual)) < 0.01 else "FAIL"
        ok &= status == "PASS"
        conn.execute("INSERT INTO recon_results VALUES (?,?,?,?,?)", (run_id, name, expected, actual, status))
        (log.info if status == "PASS" else log.error)("recon %-4s %s (expected %s, actual %s)", status, name, expected, actual)
    return ok


# ----------------------------------------------------------------------------- document
def write_docs(conn, run_id, docs_dir):
    """Generate data-lineage / data-quality documentation from the code's own metadata."""
    docs = Path(docs_dir)
    docs.mkdir(parents=True, exist_ok=True)
    lines = ["# Data lineage & data quality (auto-generated)", "", f"Last run: `{run_id}`", "",
             "## Lineage: source field -> warehouse column", "", "| Target | Source | Transformation / rule |", "|---|---|---|"]
    lines += [f"| {t} | {s} | {r} |" for t, s, r in LINEAGE]
    lines += ["", "## Data-quality rules", "", "| Source | Rule | Severity | Failing condition |", "|---|---|---|---|"]
    lines += [f"| {s} | {r} | {sev} | `{p}` |" for s, r, sev, p in RULES]
    lines += ["", "## Latest run: rule hits", "", "| Rule | Rows |", "|---|---|"]
    for rule, n in conn.execute("SELECT rule, COUNT(*) FROM dq_rejects WHERE run_id=? GROUP BY rule ORDER BY rule", (run_id,)):
        lines.append(f"| {rule} | {n} |")
    lines += ["", "## Latest run: reconciliation", "", "| Check | Expected | Actual | Status |", "|---|---|---|---|"]
    for name, e, a, st in conn.execute("SELECT check_name, expected, actual, status FROM recon_results WHERE run_id=?", (run_id,)):
        lines.append(f"| {name} | {e:g} | {a:g} | {st} |")
    (docs / "data_lineage.md").write_text("\n".join(lines) + "\n")


# ----------------------------------------------------------------------------- orchestration
def run_pipeline(raw_dir="data/raw", db_path="warehouse.db", docs_dir="docs"):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:6]
    conn = sqlite3.connect(db_path)
    conn.executescript(WAREHOUSE_DDL)
    conn.execute("INSERT INTO pipeline_runs VALUES (?,?,NULL,'RUNNING')", (run_id, _now()))
    status = "FAILED"
    try:
        log.info("run %s: extract", run_id)
        counts = extract(conn, raw_dir)
        log.info("run %s: validate", run_id)
        validate(conn, run_id)
        log.info("run %s: load", run_id)
        load(conn, run_id)
        log.info("run %s: reconcile", run_id)
        status = "SUCCESS" if reconcile(conn, run_id, counts) else "FAILED_RECONCILIATION"
        write_docs(conn, run_id, docs_dir)
    except Exception:
        log.exception("run %s failed", run_id)
        raise
    finally:
        conn.execute("UPDATE pipeline_runs SET finished_at=?, status=? WHERE run_id=?", (_now(), status, run_id))
        conn.commit()
        summary = {
            "run_id": run_id, "status": status,
            "rejected_records": conn.execute("SELECT COUNT(DISTINCT source || ':' || src_row) FROM dq_rejects WHERE run_id=? AND severity='reject'", (run_id,)).fetchone()[0],
            "fact_rows": conn.execute("SELECT COUNT(*) FROM fact_sales_orders").fetchone()[0],
        }
        conn.close()
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    result = run_pipeline(*sys.argv[1:4])
    log.info("done: %s", result)
    sys.exit(0 if result["status"] == "SUCCESS" else 1)
