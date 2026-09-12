#!/usr/bin/env python3
"""Walk a parquet file's on-disk layout: magic, trailer, footer, per-column metadata."""
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.meta import file_size, footer_length  # noqa: E402


def main(path: str) -> None:
    size = file_size(path)
    flen = footer_length(path)
    with open(path, "rb") as f:
        head = f.read(4)
        f.seek(-8, 2)
        trailer = f.read(8)

    print(f"file             : {path}")
    print(f"size             : {size} bytes")
    print(f"header magic     : {head!r}  (offset 0)")
    print(f"trailer          : {trailer!r}")
    print(f"footer length    : {flen} bytes ({flen / size:.1%} of file)")
    print(f"footer starts at : offset {size - 8 - flen}")
    print()

    pf = pq.ParquetFile(path)
    md = pf.metadata
    print(f"rows={md.num_rows}  row_groups={md.num_row_groups}  columns={md.num_columns}")
    print(f"created_by={md.created_by}")
    print()

    for rg in range(md.num_row_groups):
        rgmd = md.row_group(rg)
        print(f"row group {rg}: {rgmd.num_rows} rows, {rgmd.total_byte_size} bytes")
        for c in range(rgmd.num_columns):
            col = rgmd.column(c)
            enc = ",".join(col.encodings) if col.encodings else "-"
            st = col.statistics
            mm = (
                f"min={st.min!r} max={st.max!r} nulls={st.null_count}"
                if st is not None and st.has_min_max
                else "no min/max"
            )
            print(f"  {col.path_in_schema:<11} {col.physical_type:<14} comp={col.compression:<7} enc=[{enc}]")
            print(f"  {'':<11} {mm}")
            print(
                f"  {'':<11} has_dict={col.has_dictionary_page} "
                f"dict_off={col.dictionary_page_offset} data_off={col.data_page_offset} "
                f"csize={col.total_compressed_size} usize={col.total_uncompressed_size}"
            )
        print()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/tiny.parquet")