#!/usr/bin/env python3
"""Tabulate every page of a Parquet file.

Walks each column chunk page by page using the hand-written Thrift reader and
writes one row per page to data/results_pages.csv:

  row_group, column, page, offset, type, encoding, num_values, nulls,
  header_bytes, compressed_bytes, uncompressed_bytes, index_bit_width,
  max_index

It also writes data/results_column_bytes.csv, one row per column, with the
column's size at three stages: in memory (Arrow), after encoding (sum of
uncompressed page sizes) and after compression (sum of compressed page sizes).

    python scripts/page_stats.py [FILE]          (default: data/big.parquet)
"""
import csv
import struct
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pqlab.footer import read_footer  # noqa: E402
from pqlab.thrift import CompactReader  # noqa: E402
from structure import CODEC, ENCODING, PAGE_TYPE, Column, decompress, hybrid_runs  # noqa: E402

PAGES_OUT = "data/results_pages.csv"
COLUMNS_OUT = "data/results_column_bytes.csv"


def walk(buf, footer):
    rows = []
    for ref in footer.chunks:
        col = Column(ref.schema_element, ref.meta)
        pos, end, page_no = col.start, col.start + col.csize, 0
        while pos < end:
            page_no += 1
            r = CompactReader(buf, pos)
            ph = r.read_struct()
            hdr_end = r.pos
            ptype, usize, csize = ph.get(1), ph.get(2), ph.get(3)
            sub = ph.get(7) if ptype == 2 else ph.get(5)
            enc = sub.get(2) if sub else None
            num = sub.get(1) if sub else 0
            nulls, bit_width, max_index = "", "", ""

            if ptype == 0:                                   # V1 data page: read levels + bit width
                body = buf[hdr_end:hdr_end + csize]
                data = decompress(col.codec, body, usize) if col.codec else body
                p = 0
                if col.max_def > 0:
                    n_def = struct.unpack_from("<I", data, 0)[0]
                    _, defs = hybrid_runs(data[4:4 + n_def], 1, num, lambda q: q)
                    nulls = defs.count(0)
                    p = 4 + n_def
                else:
                    nulls = 0
                if enc in (2, 8):
                    bit_width = data[p]
                    present = num - (nulls or 0)
                    _, idxs = hybrid_runs(data[p + 1:], bit_width, present, lambda q: q)
                    max_index = max(idxs) if idxs else 0

            rows.append({
                "row_group": ref.row_group, "column": col.name, "page": page_no,
                "offset": pos, "type": PAGE_TYPE.get(ptype, ptype),
                "encoding": ENCODING.get(enc, enc), "num_values": num, "nulls": nulls,
                "header_bytes": hdr_end - pos, "compressed_bytes": csize,
                "uncompressed_bytes": usize, "index_bit_width": bit_width,
                "max_index": max_index,
            })
            pos = hdr_end + csize
    return rows


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "data/big.parquet"
    buf = Path(path).read_bytes()
    footer = read_footer(buf)
    rows = walk(buf, footer)

    with open(PAGES_OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    # per-column bytes at three stages
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    encoded, compressed = defaultdict(int), defaultdict(int)
    for r in rows:
        encoded[r["column"]] += r["uncompressed_bytes"] + r["header_bytes"]
        compressed[r["column"]] += r["compressed_bytes"] + r["header_bytes"]
    names = [se.get(4).decode() for se in footer.leaves]
    with open(COLUMNS_OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["column", "arrow_bytes", "encoded_bytes", "compressed_bytes"])
        for n in names:
            w.writerow([n, table.column(n).nbytes, encoded[n], compressed[n]])

    # summary
    codec = CODEC.get(Column(footer.chunks[0].schema_element, footer.chunks[0].meta).codec)
    print(f"File: {path}")
    print(f"Rows: {footer.num_rows}   Row groups: {len(footer.row_groups)}   "
          f"Pages: {len(rows)}   Codec: {codec}")
    print()
    print(f"  {'Column':<12}{'Pages':>6}{'Dict':>6}{'RLE_DICT':>10}{'PLAIN':>7}"
          f"{'Bit width':>11}{'Encoded B':>12}{'Stored B':>11}")
    by_col = defaultdict(list)
    for r in rows:
        by_col[r["column"]].append(r)
    for n in names:
        rs = by_col[n]
        kinds = Counter((r["type"], r["encoding"]) for r in rs)
        widths = sorted({r["index_bit_width"] for r in rs if r["index_bit_width"] != ""})
        width = f"{widths[0]}-{widths[-1]}" if len(widths) > 1 else (str(widths[0]) if widths else "-")
        print(f"  {n:<12}{len(rs):>6}{kinds[('DICTIONARY_PAGE', 'PLAIN')]:>6}"
              f"{kinds[('DATA_PAGE', 'RLE_DICTIONARY')]:>10}{kinds[('DATA_PAGE', 'PLAIN')]:>7}"
              f"{width:>11}{encoded[n]:>12}{compressed[n]:>11}")
    print()
    print(f"wrote {PAGES_OUT} ({len(rows)} rows)")
    print(f"wrote {COLUMNS_OUT} ({len(names)} rows)")


if __name__ == "__main__":
    main()