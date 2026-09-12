#!/usr/bin/env python3
'''
Fixing the value of N, we vary the row_group_size to see how it affects the number of row groups, the size of the footer, and the overall file size. This helps in understanding the trade-offs between having many small row groups (which can improve query performance due to better pushdown capabilities) versus having fewer larger row groups (which can reduce footer overhead).
'''
import csv
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402
from pqlab.meta import footer_length  # noqa: E402

N = 1_000_000
GROUP_SIZES = [1_000, 10_000, 100_000, 250_000, 1_000_000]
OUT = "data/results_rowgroups.csv"


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    t = build_table(N)
    rows = []
    for g in GROUP_SIZES:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            path = tmp.name
        pq.write_table(t, path, compression="snappy", row_group_size=g, write_statistics=True)
        md = pq.ParquetFile(path).metadata
        rec = {
            "row_group_size": g,
            "num_row_groups": md.num_row_groups,
            "file_bytes": Path(path).stat().st_size,
            "footer_bytes": footer_length(path),
        }
        Path(path).unlink()
        rows.append(rec)
        print(rec)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()