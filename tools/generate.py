#!/usr/bin/env python3
"""Synthetic payment generator: POSTs random payments to the gateway.

    tools/generate.py [--count 100] [--invalid-rate 0.02] [--rate 20] [--url http://localhost:8080]

Accounts ACC-000001..ACC-001000 exist; currency is GBP if n%3==0, EUR if n%3==1, USD if n%3==2.
About --invalid-rate of the payments are deliberately bad in a way only the processor can detect
(currency mismatch, unknown account, over the limit, same account). Standard library only.
"""
import argparse
import json
import random
import time
import urllib.error
import urllib.request
import uuid

CCY = {0: "GBP", 1: "EUR", 2: "USD"}


def acc(n):
    return f"ACC-{n:06d}"


def valid_payment(rng):
    a = rng.randint(1, 1000)
    b = rng.randint(1, 1000)
    while b == a or b % 3 != a % 3:          # same currency, different account
        b = rng.randint(1, 1000)
    amount = round(rng.lognormvariate(3.5, 1.2), 2)  # mostly tens to hundreds, long tail
    return {"fromAccount": acc(a), "toAccount": acc(b), "amount": max(0.01, min(amount, 25000)), "currency": CCY[a % 3]}


def invalid_payment(rng):
    p = valid_payment(rng)
    kind = rng.choice(["currency", "unknown", "limit", "same"])
    if kind == "currency":
        p["currency"] = CCY[(int(p["fromAccount"][4:]) + 1) % 3]
    elif kind == "unknown":
        p["toAccount"] = acc(rng.randint(1001, 999999))
    elif kind == "limit":
        p["amount"] = 75000.00
    else:
        p["toAccount"] = p["fromAccount"]
    return p, kind


def post(url, payment, key):
    req = urllib.request.Request(f"{url}/payments", data=json.dumps(payment).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "Idempotency-Key": key})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status, json.loads(r.read())


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--invalid-rate", type=float, default=0.02)
    ap.add_argument("--rate", type=float, default=20, help="payments per second")
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--seed", type=int)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    sent = {"valid": 0, "invalid": 0, "errors": 0}
    last_id = None
    for _ in range(a.count):
        bad = rng.random() < a.invalid_rate
        p, _kind = invalid_payment(rng) if bad else (valid_payment(rng), None)
        try:
            _, body = post(a.url, p, f"gen-{uuid.uuid4()}")
            last_id = body["paymentId"]
            sent["invalid" if bad else "valid"] += 1
        except (urllib.error.URLError, TimeoutError) as e:
            sent["errors"] += 1
            print(f"error: {e}")
        time.sleep(1 / a.rate)
    print(json.dumps({**sent, "last_payment_id": last_id}))


if __name__ == "__main__":
    main()
