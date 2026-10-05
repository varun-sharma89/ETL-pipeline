"""Generate synthetic source extracts (with deliberately injected defects) for the pipeline.

Three 'source systems':
  * sap_sales_orders.csv  - SAP-style flat-file export of sales orders
  * crm_customers.json    - CRM customer export
  * product_master.csv    - product master data
"""
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path


def generate(out_dir="data/raw", seed=42, n_orders=2000):
    rnd = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    regions = ["North", "South", "East", "West", "Scotland", "Wales"]
    segments = ["Retail", "Wholesale", "Online"]
    categories = ["Hardware", "Software", "Services", "Accessories"]

    # --- product master (2 bad prices injected) ---
    products = [{"material_id": f"M{i:04d}", "description": f"Product {i}",
                 "category": rnd.choice(categories), "unit_price": round(rnd.uniform(5, 500), 2)}
                for i in range(1, 41)]
    products[7]["unit_price"] = -1
    products[19]["unit_price"] = ""
    good_products = [p for i, p in enumerate(products) if i not in (7, 19)]

    # --- CRM customers (10 missing regions, 5 duplicate ids) ---
    customers = [{"customer_id": f"C{i:04d}", "name": f"Customer {i}",
                  "region": rnd.choice(regions), "segment": rnd.choice(segments)}
                 for i in range(1, 151)]
    for i in (3, 17, 42, 60, 77, 90, 101, 120, 133, 140):
        customers[i]["region"] = None
    customers += [dict(customers[i]) for i in (5, 25, 55, 85, 115)]

    # --- sales orders ---
    start = date(2024, 1, 1)
    orders = []
    for i in range(1, n_orders + 1):
        p = rnd.choice(good_products)
        qty = rnd.randint(1, 20)
        orders.append({
            "order_id": f"SO{i:06d}",
            "order_date": (start + timedelta(days=rnd.randint(0, 364))).isoformat(),
            "customer_id": f"C{rnd.randint(1, 150):04d}",
            "material_id": p["material_id"],
            "quantity": qty,
            "net_value": round(qty * p["unit_price"], 2),
            "currency": "GBP",
        })

    # inject defects into distinct rows
    pool = rnd.sample(range(n_orders), 80)
    for i in pool[0:15]:  orders[i]["customer_id"] = ""            # missing customer
    for i in pool[15:25]: orders[i]["order_date"] = rnd.choice(["31/02/2024", "2024-13-01", "not a date"])
    for i in pool[25:37]: orders[i]["quantity"] = -abs(orders[i]["quantity"])
    for i in pool[37:45]: orders[i]["material_id"] = "M9999"        # not in product master
    for i in pool[45:55]: orders[i]["customer_id"] = "C9999"        # not in CRM
    for i in pool[55:60]: orders[i]["net_value"] = -abs(orders[i]["net_value"])
    orders += [dict(orders[i]) for i in rnd.sample(range(n_orders), 25)]  # 25 duplicate rows

    with open(out / "sap_sales_orders.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
        w.writeheader()
        w.writerows(orders)
    with open(out / "product_master.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(products[0].keys()))
        w.writeheader()
        w.writerows(products)
    with open(out / "crm_customers.json", "w") as f:
        json.dump(customers, f, indent=2)
    return {"orders": len(orders), "customers": len(customers), "products": len(products)}


if __name__ == "__main__":
    print(generate())
