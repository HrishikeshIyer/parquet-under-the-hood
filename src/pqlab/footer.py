"""Read a Parquet footer with the hand-written Thrift reader and list column chunks.

    buf = Path("data/big.parquet").read_bytes()
    footer = read_footer(buf)
    for chunk in footer.chunks: ...

Each chunk carries its schema element and ColumnMetaData as decoded Thrift
structs, plus its row group index and the global row range it covers.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field

from pqlab.thrift import CompactReader, Struct


@dataclass
class ChunkRef:
    row_group: int
    row_start: int          # first row of this row group, counted across the file
    row_count: int
    schema_element: Struct
    meta: Struct            # ColumnMetaData


@dataclass
class Footer:
    length: int
    offset: int
    meta: Struct            # FileMetaData
    leaves: list = field(default_factory=list)
    chunks: list = field(default_factory=list)

    @property
    def num_rows(self) -> int:
        return self.meta.get(3)

    @property
    def row_groups(self) -> list:
        return self.meta.get(4)


def read_footer(buf: bytes) -> Footer:
    if buf[:4] != b"PAR1" or buf[-4:] != b"PAR1":
        raise ValueError("missing PAR1 magic; not a Parquet file")
    length = struct.unpack("<I", buf[-8:-4])[0]
    offset = len(buf) - 8 - length
    fm = CompactReader(buf, offset).read_struct()
    footer = Footer(length=length, offset=offset, meta=fm)
    footer.leaves = [se for se in fm.get(2) if se.get(5) is None]
    row_start = 0
    for g, rg in enumerate(fm.get(4)):
        for se, cc in zip(footer.leaves, rg.get(1)):
            footer.chunks.append(ChunkRef(g, row_start, rg.get(3), se, cc.get(3)))
        row_start += rg.get(3)
    return footer