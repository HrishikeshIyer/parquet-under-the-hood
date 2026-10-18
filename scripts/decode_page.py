#!/usr/bin/env python3
"""Decode a dictionary-encoded column by hand: dictionary page + data page.

Reconstructs the column's values straight from the bytes — PLAIN dictionary,
then the data page's definition levels + RLE/bit-packed indices — and asserts
the result equals what PyArrow reads. Written for a flat, non-null column
(e.g. `category`); nulls would need the definition levels decoded too.

    python scripts/decode_page.py [column]   # default: category
"""
import struct
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.thrift import CompactReader  # noqa: E402
from pqlab.encoding import read_plain_byte_array, rle_bitpack_hybrid  # noqa: E402

PATH = "data/tiny.parquet"
ENC = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 8: "RLE_DICTIONARY"}


def main(column: str = "category") -> None:
    buf = Path(PATH).read_bytes()
    rg = pq.ParquetFile(PATH).metadata.row_group(0)
    col = next(rg.column(i) for i in range(rg.num_columns)
               if rg.column(i).path_in_schema == column)

    # 1. dictionary page -> the distinct values (PLAIN)
    dr = CompactReader(buf, col.dictionary_page_offset)
    dph = dr.read_struct()
    n_dict = dph.get(7).get(1)
    raw, _ = read_plain_byte_array(buf, dr.pos, n_dict)
    dictionary = [b.decode() for b in raw]
    print(f"dictionary ({n_dict} values): {dictionary}")

    # 2. data page -> definition levels + RLE/bit-packed indices
    r = CompactReader(buf, col.data_page_offset)
    ph = r.read_struct()
    dp = ph.get(5)                       # DataPageHeader
    n = dp.get(1)
    print(f"data page: {n} values, encoding={ENC.get(dp.get(2))}, "
          f"def_levels={ENC.get(dp.get(3))}")

    p = r.pos
    def_len = struct.unpack_from("<I", buf, p)[0]; p += 4
    p += def_len                          # skip def levels (no nulls in this column)
    bit_width = buf[p]; p += 1
    print(f"def-levels: {def_len}-byte RLE block (all present)   index bit width: {bit_width}")

    indices = rle_bitpack_hybrid(buf[p:r.pos + ph.get(3)], bit_width, n)
    values = [dictionary[i] for i in indices]
    print(f"indices (first 12): {indices[:12]}")
    print(f"values  (first 8) : {values[:8]}")

    truth = pq.read_table(PATH, columns=[column]).column(0).to_pylist()
    assert values == truth, "decoded values do not match PyArrow!"
    print("\nOK — hand-decoded column matches PyArrow exactly.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "category")