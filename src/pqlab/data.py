'''
This file builds the data schema and uses various data types.

| Column | Arrow type | Parquet physical | Illustrates |
|---|---|---|---|
| `id` | int32 (sorted) | INT32 | tight min/max stats, dictionary/RLE |
| `event_ts` | timestamp(us) | INT64 (TIMESTAMP) | logical type on INT64 |
| `event_date` | date32 | INT32 (DATE) | logical type on INT32 |
| `price` | float64 | DOUBLE | IEEE 754, no dictionary win |
| `quantity` | int64 | INT64 | plain integers |
| `category` | string (4 values) | BYTE_ARRAY | **dictionary encoding** |
| `city` | string (5 values) | BYTE_ARRAY | dictionary encoding |
| `sku` | string (unique) | BYTE_ARRAY | high cardinality, plain fallback |
| `in_stock` | bool | BOOLEAN | bit-packing |
| `discount` | float64, ~30% null | DOUBLE | **definition levels** (nulls) |
| `payload` | binary | BYTE_ARRAY | raw bytes (no text form) |
| `rating` | decimal128(4,2) | FIXED_LEN_BYTE_ARRAY | DECIMAL logical type |

'''

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import numpy as np
import pyarrow as pa

CATEGORIES = ["alpha", "bravo", "charlie", "delta"]      # low cardinality -> dictionary
CITIES = ["London", "Berlin", "Tokyo", "Paris", "Stockholm"]     # low cardinality

# Returns an n-row table consisting of various data types
def build_table(n: int, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed)
    idx = np.arange(n, dtype=np.int32)                     # INT32, sorted -> tight stats
    base = dt.datetime(2024, 1, 1)

    ts = [base + dt.timedelta(minutes=int(m)) for m in idx]            # INT64 TIMESTAMP(us)
    dates = [(base + dt.timedelta(days=int(i % 365))).date() for i in idx]  # INT32 DATE
    price = rng.normal(100, 25, n).round(2)                            # DOUBLE (IEEE 754)
    qty = rng.integers(1, 50, n, dtype=np.int64)                       # INT64
    category = [CATEGORIES[i % len(CATEGORIES)] for i in idx]          # BYTE_ARRAY -> dict
    city = [CITIES[int(rng.integers(0, len(CITIES)))] for _ in idx]    # BYTE_ARRAY -> dict
    sku = [f"SKU-{int(rng.integers(0, 2**31)):08x}" for _ in idx]      # BYTE_ARRAY
    in_stock = rng.integers(0, 2, n).astype(bool)                      # BOOLEAN (bit-packed)
    disc = rng.random(n)                                               # ~30% nulls -> def levels
    discount = pa.array(
        [None if d < 0.30 else round(float(d), 3) for d in disc], type=pa.float64()
    )
    payload = [rng.bytes(4) for _ in idx]                             # BYTE_ARRAY (raw bytes)
    rating = pa.array(
        [Decimal(f"{rng.uniform(1, 5):.2f}") for _ in idx], type=pa.decimal128(4, 2)
    )                                                                 # FIXED_LEN_BYTE_ARRAY DECIMAL

    return pa.table(
        {
            "id": pa.array(idx, type=pa.int32()),
            "event_ts": pa.array(ts, type=pa.timestamp("us")),
            "event_date": pa.array(dates, type=pa.date32()),
            "price": pa.array(price, type=pa.float64()),
            "quantity": pa.array(qty, type=pa.int64()),
            "category": pa.array(category, type=pa.string()),
            "city": pa.array(city, type=pa.string()),
            "sku": pa.array(sku, type=pa.string()),
            "in_stock": pa.array(in_stock, type=pa.bool_()),
            "discount": discount,
            "payload": pa.array(payload, type=pa.binary()),
            "rating": rating,
        }
    )