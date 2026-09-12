#!/usr/bin/env python3
'''
Compares CSV and Parquet file sizes and metadata for a ranges of rows.
'''
import csv
import io
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402
from pqlab.meta import footer_length  # noqa: E402

ROWS = [10, 100, 1_000, 10_000, 100_000, 1_000_000]
OUT = "data/results.csv"


def parquet_bytes(table: pa.Table, **kw) -> tuple[int, int]:
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = tmp.name
    pq.write_table(table, path, **kw)
    size = Path(path).stat().st_size
    flen = footer_length(path)
    Path(path).unlink()
    return size, flen


def csv_bytes(table: pa.Table) -> int:
    buf = io.BytesIO()
    pacsv.write_csv(table, buf)
    return buf.tell()


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    rows = []
    for n in ROWS:
        # CSV cannot represent the raw-binary column; project it out so both
        # formats carry identical columns for an apples-to-apples size comparison.
        t = build_table(n).drop_columns(["payload"])
        csv_sz = csv_bytes(t)
        pq_snappy, footer = parquet_bytes(
            t, compression="snappy", use_dictionary=True, write_statistics=True
        )
        pq_none, _ = parquet_bytes(t, compression="none", use_dictionary=True)
        rec = {
            "rows": n,
            "parquet_snappy": pq_snappy,
            "parquet_none": pq_none,
            "csv": csv_sz,
            "footer": footer,
            "footer_pct": round(100 * footer / pq_snappy, 2),
            "parquet_vs_csv": round(pq_snappy / csv_sz, 3),
        }
        rows.append(rec)
        print(rec)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()