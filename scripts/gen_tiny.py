#!/usr/bin/env python3
'''
This file generates a small, uncompressed Parquet file with a variety of data types (refer /src/pqlab/data.py for the schema)
'''
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402

OUT = "data/tiny.parquet"


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    table = build_table(n=64, seed=7)
    pq.write_table(
        table,
        OUT,
        compression="none",     # raw bytes stay readable in a hex dump
        use_dictionary=True,     # low-cardinality columns get a dictionary page
        write_statistics=True,   # min/max/null_count land in the footer
        version="2.6",
        row_group_size=64,       # one row group
    )
    print(f"wrote {OUT} ({Path(OUT).stat().st_size} bytes)")


if __name__ == "__main__":
    main()