#!/usr/bin/env python3
"""This file generates a CSV file and its corresponding Parquet file (not compressed, dictionary-encoded, one row group) for testing purposes.

- data/tiny.parquet : uncompressed, dictionary-encoded, one row group.
                      so encoded bytes stay legible in a hex dump.
- data/tiny.csv     : the same rows in plain text.
"""
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402

PARQUET_OUT = "data/tiny.parquet"
CSV_OUT = "data/tiny.csv"


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    table = build_table(64, seed=7)

    # Parquet
    pq.write_table(
        table, PARQUET_OUT,
        compression="none",      # raw bytes stay readable
        use_dictionary=True,      # low-cardinality columns get a dictionary page
        write_statistics=True,
        version="2.6",
        row_group_size=64,        # one row group
    )
    print(f"wrote {PARQUET_OUT} ({Path(PARQUET_OUT).stat().st_size} bytes)")

    # CSV
    hex_payload = pa.array([b.hex() for b in table.column("payload").to_pylist()])
    csv_table = table.set_column(
        table.schema.get_field_index("payload"), "payload_hex", hex_payload
    )
    pacsv.write_csv(csv_table, CSV_OUT)
    print(f"wrote {CSV_OUT} ({Path(CSV_OUT).stat().st_size} bytes)")


if __name__ == "__main__":
    main()