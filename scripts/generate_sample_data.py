#!/usr/bin/env python3
"""Generate small, deterministic input CSVs for FraudDetectionApp.

Writes into ./data:
  accounts.csv      account_id, customer_name, branch, opened_on
  blacklist.csv     account_id, reason
  transactions.csv  txn_id, account_id, amount, timestamp (epoch ms), txn_type
  2019-Nov.csv      e-commerce events in the Kaggle "2019-Nov" schema (synthetic)

Usage: python3 scripts/generate_sample_data.py [--events 200000]
"""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)
OUT = "data"

FIRST = ["Asha", "Ravi", "Meera", "John", "Lena", "Omar", "Priya", "Chen", "Sara", "Diego"]
LAST = ["Rao", "Kumar", "Smith", "Ito", "Garcia", "Khan", "Patel", "Nguyen", "Brown", "Silva"]
REASONS = ["OFAC sanctions match", "Known mule account", "Chargeback abuse", "Identity theft report"]
TXN_TYPES = ["POS", "ONLINE", "WIRE", "ATM"]
CATEGORIES = ["electronics.smartphone", "electronics.audio", "appliances.kitchen",
              "computers.notebook", "apparel.shoes", "furniture.living_room", "sport.bicycle"]
BRANDS = ["samsung", "apple", "xiaomi", "sony", "bosch", "lenovo", "nike", ""]


def write(name, header, rows):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        n = 0
        for r in rows:
            w.writerow(r)
            n += 1
    print(f"wrote {path} ({n:,} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--accounts", type=int, default=2000)
    ap.add_argument("--transactions", type=int, default=100000)
    ap.add_argument("--events", type=int, default=200000)
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)

    acct_ids = [f"ACC{i:06d}" for i in range(a.accounts)]
    write("accounts.csv", ["account_id", "customer_name", "branch", "opened_on"],
          ((acc, f"{random.choice(FIRST)} {random.choice(LAST)}", f"BR{random.randint(1, 40):03d}",
            (datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3000))).date().isoformat())
           for acc in acct_ids))

    write("blacklist.csv", ["account_id", "reason"],
          ((acc, random.choice(REASONS)) for acc in random.sample(acct_ids, a.accounts // 50)))

    base_ms = int(datetime(2024, 6, 1).timestamp() * 1000)

    def txns():
        t = base_ms
        for i in range(a.transactions):
            t += random.randint(1, 20) * 1000          # bursts within 60 s trigger "rapid-fire" rule
            amount = round(random.lognormvariate(5.5, 1.2), 2)
            yield (f"T{i:08d}", random.choice(acct_ids), amount, t, random.choice(TXN_TYPES))
    write("transactions.csv", ["txn_id", "account_id", "amount", "timestamp", "txn_type"], txns())

    start = datetime(2019, 11, 1)

    def events():
        sessions = [f"s-{i:07d}" for i in range(a.events // 8)]
        for _ in range(a.events):
            ts = start + timedelta(seconds=random.randint(0, 29 * 86400))
            et = random.choices(["view", "cart", "purchase"], weights=[85, 10, 5])[0]
            cat = random.choice(CATEGORIES)
            yield (ts.strftime("%Y-%m-%d %H:%M:%S") + " UTC", et, random.randint(1000000, 1009999),
                   random.randint(2053013550000000000, 2053013559999999999),
                   cat if random.random() > 0.2 else "", random.choice(BRANDS),
                   round(random.uniform(5, 1500), 2), random.randint(500000000, 500099999),
                   random.choice(sessions))
    write("2019-Nov.csv", ["event_time", "event_type", "product_id", "category_id", "category_code",
                           "brand", "price", "user_id", "user_session"], events())


if __name__ == "__main__":
    main()
