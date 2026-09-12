'''
This file helps to read the metadata of the Parquet file. By reading the last 8 bytes of the file, 
we can ensure the metadata overhead measurement is same across various file sizes.
'''

from __future__ import annotations

import struct
from pathlib import Path

import pyarrow.parquet as pq

MAGIC = b"PAR1"


def footer_length(path: str | Path) -> int:
    with open(path, "rb") as f:
        f.seek(-8, 2)
        tail = f.read(8)
    if tail[4:] != MAGIC:
        raise ValueError(f"{path}: bad trailer magic {tail[4:]!r}")
    return struct.unpack("<I", tail[:4])[0]


def file_size(path: str | Path) -> int:
    return Path(path).stat().st_size


def summarize(path: str | Path) -> dict:
    md = pq.ParquetFile(path).metadata
    return {
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "columns": md.num_columns,
        "file_bytes": file_size(path),
        "footer_bytes": footer_length(path),
    }