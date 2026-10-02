"""Deterministic retail dataset generator.

Produces customers/products/orders/order_items CSV batches so the pipeline
can be run incrementally (batch 1 = first half, batch 2 = second half) with
demonstrable dimension changes for SCD Type 2.
"""

from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
CUSTOMERS = 500
PRODUCTS = 200
CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Denpasar", "Palembang"]
SEGMENTS = ["consumer", "professional", "enterprise"]
CHANNELS = ["marketplace", "website", "pos"]
STATUSES = ["completed", "completed", "completed", "cancelled", "refunded"]
CATEGORIES = {
    "apparel": ["shirt", "pants", "jacket", "dress", "shoes"],
    "electronics": ["headphones", "charger", "speaker", "watch", "cable"],
    "home": ["lamp", "cushion", "vase", "blanket", "organizer"],
    "beauty": ["serum", "moisturizer", "sunscreen", "cleanser", "mask"],
}


def rng(seed: int) -> random.Random:
    return random.Random(seed)


def generate_customers(out: Path, batch: int, months: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    today = date(2026, 6, 30) if batch == 1 else date(2026, 12, 31)
    start = date(2024, 1, 1)
    days = (today - start).days
    rows = []
    for cid in range(1, CUSTOMERS + 1):
        rr = rng(SEED + cid * 31)
        city = rr.choice(CITIES)
        segment = rr.choice(SEGMENTS)
        if batch == 2 and cid % 10 == 0:
            segment = rr.choice([s for s in SEGMENTS if s != segment])
        if batch == 2 and cid % 7 == 0:
            city = rr.choice([c for c in CITIES if c != city])
        rows.append(
            [
                cid,
                f"customer_{cid:04d}",
                city,
                segment,
                (start + timedelta(days=rr.randint(0, days))).isoformat(),
            ]
        )
    with open(out / f"customers_b{batch}.csv", "w", newline="") as f:
        csv.writer(f).writerows([["customer_id", "name", "city", "segment", "signup_date"]] + rows)


def generate_products(out: Path, batch: int, months: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    pid = 1
    for cat, subcats in CATEGORIES.items():
        for sub in subcats:
            for variant in range(10):
                rr = rng(SEED * 2 + pid * 17)
                price = round(rr.uniform(50_000, 3_000_000), -3)
                cost = round(price * rr.uniform(0.45, 0.7))
                launch = date(2026, rr.randint(1, 12), rr.randint(1, 28))
                rows.append(
                    [
                        pid,
                        f"{cat[:3].upper()}-{sub[:2].upper()}-{variant + 1:02d}",
                        f"{sub.title()} {variant + 1}",
                        cat,
                        sub,
                        price,
                        cost,
                        launch.isoformat(),
                    ]
                )
                pid += 1
    with open(out / f"products_b{batch}.csv", "w", newline="") as f:
        f.write("product_id,sku,name,category,subcategory,unit_price,cost,launch_date\n")
        writer = csv.writer(f)
        writer.writerows(rows)


def generate_orders(out: Path, batch: int, months: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    r = rng(SEED * 3 + batch)
    with open(out / f"products_b{batch}.csv") as f:
        products = {int(row["product_id"]): float(row["unit_price"]) for row in csv.DictReader(f)}
    first_month = (batch - 1) * months + 1
    end_month = batch * months
    start = date(2026, first_month, 1)
    end = date(2026, end_month, 28)
    oid = (batch - 1) * 50_000 + 1
    order_rows = []
    item_rows = []
    day = start
    while day <= end:
        n_orders = r.randint(25, 60)
        for _ in range(n_orders):
            hour = r.randint(8, 22)
            ts = datetime(day.year, day.month, day.day, hour, r.randint(0, 59))
            customer = r.randint(1, CUSTOMERS)
            channel = r.choice(CHANNELS)
            status = r.choice(STATUSES)
            order_rows.append([oid, customer, ts.isoformat(), channel, status])
            n_items = r.randint(1, 5)
            for _ in range(n_items):
                product = r.choice(list(products))
                qty = r.randint(1, 3)
                price = products[product]
                discount = r.choice([0, 0, 0.1, 0.15, 0.25])
                unit_price = round(price * (1 - discount) * r.uniform(0.95, 1.05), -2)
                item_rows.append(
                    [uuid.uuid4().hex[:12], oid, product, qty, unit_price, discount]
                )
            oid += 1
        day += timedelta(days=1)
    with open(out / f"orders_b{batch}.csv", "w", newline="") as f:
        f.write("order_id,customer_id,order_date,channel,status\n")
        csv.writer(f).writerows(order_rows)
    with open(out / f"order_items_b{batch}.csv", "w", newline="") as f:
        f.write("order_item_id,order_id,product_id,quantity,unit_price_at_sale,discount_pct\n")
        csv.writer(f).writerows(item_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=int, required=True, choices=[1, 2])
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--out", type=Path, default=Path(".data/raw"))
    args = parser.parse_args()

    out_dir = args.out / f"batch{args.batch}"
    out_dir.mkdir(parents=True, exist_ok=True)
    generate_customers(out_dir, args.batch, args.months)
    generate_products(out_dir, args.batch, args.months)
    generate_orders(out_dir, args.batch, args.months)
    print(f"generated batch {args.batch} -> {out_dir}")


if __name__ == "__main__":
    main()
