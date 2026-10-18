#!/usr/bin/env python3
"""Generate the large sample: data/big.parquet and its CSV twin data/big.csv.

Same 12-column schema as the tiny file, at 1,000,000 rows. The Parquet file
uses ZSTD compression and 250,000-row row groups (four row groups); page and
dictionary size limits are left at PyArrow's defaults, so the encoding
decisions in the file are the writer's own.

The CSV twin writes the binary `payload` column as hex, since CSV is text only.

    python scripts/gen_big.py [--rows N] [--row-group-size N]
"""
import argparse
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402

PARQUET_OUT = "data/big.parquet"
CSV_OUT = "data/big.csv"


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate data/big.parquet and data/big.csv.")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--row-group-size", type=int, default=250_000)
    args = ap.parse_args()

    Path("data").mkdir(exist_ok=True)
    t0 = time.perf_counter()
    table = build_table(args.rows, seed=42)
    print(f"built table: {args.rows} rows in {time.perf_counter() - t0:.1f} s")

    pq.write_table(table, PARQUET_OUT, compression="zstd",
                   use_dictionary=True, write_statistics=True,
                   row_group_size=args.row_group_size)
    print(f"wrote {PARQUET_OUT} ({Path(PARQUET_OUT).stat().st_size} bytes)")

    hex_payload = pa.array([b.hex() for b in table.column("payload").to_pylist()])
    csv_table = table.set_column(table.schema.get_field_index("payload"), "payload_hex", hex_payload)
    pacsv.write_csv(csv_table, CSV_OUT)
    print(f"wrote {CSV_OUT} ({Path(CSV_OUT).stat().st_size} bytes)")


if __name__ == "__main__":
    main()