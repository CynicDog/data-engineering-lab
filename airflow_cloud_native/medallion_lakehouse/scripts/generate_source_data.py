"""Generate synthetic 'source system' CSV files into ./source_data.

Run this once before bringing the stack up. The ingest DAG reads from
./source_data and writes Bronze partitions on MinIO.

Usage:
    uv run python scripts/generate_source_data.py --days 7 --customers 200 --orders 4000
"""
from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


COUNTRIES = ["US", "DE", "FR", "JP", "GB", "CA"]
CATEGORIES = ["electronics", "apparel", "home", "books", "outdoor"]
ACTIONS = ["view", "click", "scroll", "add_to_cart", "purchase"]
STATUS_RAW = ["new", "created", "paid", "shipped", "cancelled", "canceled", "refunded"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=200)
    parser.add_argument("--products", type=int, default=50)
    parser.add_argument("--orders", type=int, default=4000)
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out = Path(__file__).resolve().parent.parent / "source_data"
    out.mkdir(exist_ok=True)
    rng = random.Random(args.seed)
    now = datetime.now(tz=timezone.utc)

    # --- customers
    customers = []
    for cid in range(1, args.customers + 1):
        customers.append(
            (
                f"c{cid:05d}",
                f"Customer {cid:05d}",
                rng.choice(COUNTRIES),
                (now - timedelta(days=rng.randint(30, 1000))).isoformat(),
            )
        )
    with (out / "customers.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "name", "country", "signed_up_at"])
        w.writerows(customers)

    # --- products
    products = []
    for pid in range(1, args.products + 1):
        products.append(
            (
                f"p{pid:04d}",
                f"Product {pid:04d}",
                rng.choice(CATEGORIES),
                round(rng.uniform(5.0, 500.0), 2),
            )
        )
    with (out / "products.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "name", "category", "list_price"])
        w.writerows(products)

    # --- orders (with a sprinkling of duplicate order_ids = retries)
    orders = []
    for _ in range(args.orders):
        cid = rng.choice(customers)[0]
        ts = now - timedelta(seconds=rng.randint(0, args.days * 86400))
        orders.append(
            (
                str(uuid.uuid4()),
                cid,
                rng.choices(STATUS_RAW, weights=[1, 1, 4, 3, 1, 1, 1])[0],
                round(rng.uniform(5.0, 800.0), 2),
                ts.isoformat(),
            )
        )
    # 2% retries — same order_id appears again with a later status
    retries = []
    for _ in range(args.orders // 50):
        original = rng.choice(orders)
        retries.append(
            (
                original[0],
                original[1],
                rng.choice(["paid", "shipped", "refunded"]),
                original[3],
                (datetime.fromisoformat(original[4]) + timedelta(hours=1)).isoformat(),
            )
        )
    with (out / "orders.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "status", "amount", "event_ts"])
        w.writerows(orders + retries)

    # --- web_events
    events = []
    customer_ids = [c[0] for c in customers]
    product_ids = [p[0] for p in products]
    for _ in range(args.events):
        ts = now - timedelta(seconds=rng.randint(0, args.days * 86400))
        events.append(
            (
                str(uuid.uuid4()),
                rng.choice(customer_ids),
                rng.choice(product_ids) if rng.random() < 0.8 else "",
                rng.choices(ACTIONS, weights=[5, 3, 2, 1, 1])[0],
                ts.isoformat(),
            )
        )
    with (out / "web_events.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "customer_id", "product_id", "action", "event_ts"])
        w.writerows(events)

    print(
        f"wrote {len(customers)} customers, {len(products)} products, "
        f"{len(orders) + len(retries)} orders ({len(retries)} retries), "
        f"{len(events)} web_events → {out}"
    )


if __name__ == "__main__":
    main()
