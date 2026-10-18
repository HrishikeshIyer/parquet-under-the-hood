#!/usr/bin/env python3
"""Show that compression wraps encoding: same page, readable vs opaque.

Dumps the `category` dictionary page from the uncompressed file and from the
snappy file, decompresses the snappy body, and asserts it equals the
uncompressed body byte for byte.
"""
import sys
from pathlib import Path

import cramjam
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.thrift import CompactReader  # noqa: E402
from hexdump import hexdump  # noqa: E402  (scripts/ is on sys.path when run from repo root)

U = "data/tiny.parquet"
C = "data/tiny_encoded_compressed.parquet"


def dict_offset(path, column):
    rg = pq.ParquetFile(path).metadata.row_group(0)
    col = next(rg.column(i) for i in range(rg.num_columns)
               if rg.column(i).path_in_schema == column)
    return col.dictionary_page_offset


def page_body(path, off):
    buf = Path(path).read_bytes()
    r = CompactReader(buf, off)
    ph = r.read_struct()                     # parse the Thrift page header
    usize, csize = ph.get(2), ph.get(3)      # uncompressed_size, compressed_size
    return buf, off, r.pos, usize, csize, buf[r.pos:r.pos + csize]


def main():
    ub, uoff, ubody, uu, uc, u_body = page_body(U, dict_offset(U, "category"))
    cb, coff, cbody, cu, cc, c_body = page_body(C, dict_offset(C, "category"))

    print("UNCOMPRESSED category dict page:")
    print(hexdump(ub[uoff:ubody + uu], base=uoff))
    print(f"  header {ubody - uoff}B + body {uu}B (PLAIN, readable)\n")

    print("ENCODED+COMPRESSED category dict page:")
    print(hexdump(cb[coff:cbody + cc], base=coff))
    print(f"  header {cbody - coff}B + body {cc}B (snappy)\n")

    decompressed = bytes(cramjam.snappy.decompress_raw(c_body))
    print("snappy body decompressed == uncompressed body:", decompressed == u_body)


if __name__ == "__main__":
    main()