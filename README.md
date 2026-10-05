# Multi-Source ETL Pipeline with Data Quality & Reconciliation

A small, end-to-end data engineering project in Python and SQL (SQLite). It integrates three source
systems, validates the data, loads a star schema, proves the load is complete and documents lineage
automatically. All data is **synthetic** and generated with deliberately injected defects.

## Sources
| Source | Format | Content |
|---|---|---|
| `sap_sales_orders.csv` | CSV | SAP-style sales order export |
| `crm_customers.json` | JSON | CRM customer records |
| `product_master.csv` | CSV | Product master data |

## Flow
```
raw files -> extract -> staging (stg_*) -> validate (SQL rules) -> quarantine (dq_rejects)
                                         \-> load (idempotent upserts) -> star schema -> reconcile -> docs
```
* **Staging**: everything lands as text so bad values can be inspected rather than failing the load.
* **Validation**: 13 declarative SQL rules (missing keys, invalid dates, non-positive quantities, unknown
  products/customers, duplicates, bad prices). Failing rows go to `dq_rejects` with the rule name, so nothing
  is silently dropped. Warnings (e.g. missing region) are loaded with a default and logged.
* **Load**: `INSERT ... ON CONFLICT DO UPDATE` into `dim_customer`, `dim_product` and `fact_sales_orders`;
  re-running never creates duplicates.
* **Reconciliation**: source file rows = staged rows; staged = loaded + rejected (per source); order value
  total in source (valid rows) = warehouse total; zero orphan foreign keys. Results are stored in
  `recon_results` and the run exits non-zero if any check fails.
* **Audit & docs**: `pipeline_runs` records every run; `docs/data_lineage.md` (lineage, rules, rule hits,
  reconciliation results) is regenerated from the code's own metadata.
* **Analyst-facing view**: `v_monthly_revenue_by_region`.

## Run it
```bash
python generate_sample_data.py     # creates data/raw/*
python pipeline.py                 # runs the pipeline, writes warehouse.db and docs/data_lineage.md
python -m unittest discover tests  # 6 tests incl. idempotency and reconciliation
sqlite3 warehouse.db "SELECT * FROM v_monthly_revenue_by_region LIMIT 5;"
```
Python 3.9+ standard library only (SQLite 3.25+ for window/upsert features).

## Design decisions & next steps
* Rules are data (a list), so adding a check is one line and it is documented automatically.
* Quarantine over deletion, so data owners can fix issues at source.
* Next: swap SQLite for a cloud warehouse, add orchestration (e.g. Airflow), incremental loads on a
  watermark column, and alerting on failed reconciliation.
