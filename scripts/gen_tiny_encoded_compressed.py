#!/usr/bin/env python3
"""This file generates a Parquet file with both dictionary encoding and snappy compression.

Identical data and schema to data/tiny.parquet, but written with:
  - dictionary encoding ON  (the encoding layer: values -> dictionary + indices)
  - snappy compression  ON  (the compression layer: a generic codec on top)

"""
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402

OUT = "data/tiny_encoded_compressed.parquet"


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    pq.write_table(
        build_table(64, seed=7), OUT,
        compression="snappy",     # the compression layer
        use_dictionary=True,       # the encoding layer
        write_statistics=True,
        version="2.6",
        row_group_size=64,
    )
    print(f"wrote {OUT} ({Path(OUT).stat().st_size} bytes)")


if __name__ == "__main__":
    main()