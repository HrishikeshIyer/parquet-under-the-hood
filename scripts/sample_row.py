#!/usr/bin/env python3
"""Locate one row in the CSV and in the Parquet file, and compare access cost.

For the CSV, the row is a single line: the script finds its byte offset and
splits it into fields. For the Parquet file, each of the row's values lives in
a different column chunk: the script finds the row group, the data page that
holds the row, its definition level, its dictionary index (if any), and the
offset of the stored value, decoding everything by hand. The decoded values
are checked against the CSV line and against PyArrow.

    python scripts/sample_row.py [--row N] [--seed S]
                                 [--parquet data/big.parquet] [--csv data/big.csv]

With no --row, a row is chosen at random using --seed (default 7), so the
output is reproducible.
"""
import argparse
import random
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pqlab.footer import read_footer  # noqa: E402
from pqlab.thrift import CompactReader  # noqa: E402
ACCESS_OUT = "data/results_access.csv"

from structure import (CODEC, ENCODING, Column, decompress, fmt,  # noqa: E402
                       hybrid_runs, read_plain)


def section(title: str) -> None:
    print(f"\n{title}\n{'=' * len(title)}")


# ------------------------------------------------------------------ CSV
def csv_locate(path: str, row: int):
    """Return (header fields, line offset, line bytes, [(field offset, field bytes)])."""
    data = Path(path).read_bytes()
    header_end = data.index(b"\n")
    header = data[:header_end].decode().replace('"', "").split(",")
    pos = header_end + 1
    for _ in range(row):
        pos = data.index(b"\n", pos) + 1
    end = data.index(b"\n", pos)
    line = data[pos:end]
    fields, start = [], 0
    for part in line.split(b","):
        fields.append((pos + start, part))
        start += len(part) + 1
    return header, pos, line, fields, len(data)


# ------------------------------------------------------------------ Parquet
def parquet_locate(buf, ref, local_row):
    """Find the page holding `local_row` in one column chunk and decode its value."""
    col = Column(ref.schema_element, ref.meta)
    pos, end = col.start, col.start + col.csize
    dict_info, seen, page_no = None, 0, 0
    while pos < end:
        page_no += 1
        r = CompactReader(buf, pos)
        ph = r.read_struct()
        hdr_end = r.pos
        ptype, usize, csize = ph.get(1), ph.get(2), ph.get(3)
        if ptype == 2:                                      # remember where the dictionary is
            dict_info = (pos, hdr_end, usize, csize, ph.get(7).get(1), hdr_end - pos)
        elif ptype == 0:
            dp = ph.get(5)
            num = dp.get(1)
            if local_row < seen + num:
                return decode_value(buf, col, ph, pos, hdr_end, local_row - seen,
                                    page_no, dict_info)
            seen += num
        pos = hdr_end + csize
    raise ValueError(f"row {local_row} not found in chunk {col.name}")


def decode_value(buf, col, ph, page_off, hdr_end, k, page_no, dict_info):
    dp = ph.get(5)
    num, enc = dp.get(1), dp.get(2)
    usize, csize = ph.get(2), ph.get(3)
    body = buf[hdr_end:hdr_end + csize]
    data = decompress(col.codec, body, usize) if col.codec else body
    out = {"column": col.name, "page": page_no, "page_offset": page_off,
           "encoding": ENCODING.get(enc), "header_bytes": hdr_end - page_off,
           "page_bytes": csize, "dict_bytes": 0, "index": None}

    p = 0
    if col.max_def > 0:
        n_def = struct.unpack_from("<I", data, 0)[0]
        _, defs = hybrid_runs(data[4:4 + n_def], 1, num, lambda q: q)
        p = 4 + n_def
    else:
        defs = [0] * num
    out["def"] = defs[k]
    if defs[k] != col.max_def:
        out.update(value=None, stored="(null: no value stored)", raw=b"")
        return out
    slot = sum(1 for d in defs[:k] if d == col.max_def)       # position among non-null values
    present = sum(1 for d in defs if d == col.max_def)

    if enc in (2, 8):
        bw = data[p]
        _, idxs = hybrid_runs(data[p + 1:], bw, present, lambda q: q)
        index = idxs[slot]
        d_off, d_hdr_end, d_usize, d_csize, d_n, d_hdr = dict_info
        d_body = buf[d_hdr_end:d_hdr_end + d_csize]
        d_data = decompress(col.codec, d_body, d_usize) if col.codec else d_body
        entries, _ = read_plain(col, d_data, 0, d_n, lambda q: q)
        e_off, raw, v = entries[index]
        out.update(index=index, value=col.logical(v), raw=raw,
                   stored=f"dictionary page @{d_off}, entry {index} at +{e_off}",
                   dict_bytes=d_csize + d_hdr)
    else:
        entries, _ = read_plain(col, data, p, present, lambda q: q)
        e_off, raw, v = entries[slot]
        out.update(value=col.logical(v), raw=raw, stored=f"data page body at +{e_off}")
    return out


# ------------------------------------------------------------------ main
def main() -> None:
    ap = argparse.ArgumentParser(description="Locate one row in the CSV and Parquet files.")
    ap.add_argument("--row", type=int, help="row number (0-based); default: random")
    ap.add_argument("--seed", type=int, default=7, help="seed for the random row (default: 7)")
    ap.add_argument("--parquet", default="data/big.parquet")
    ap.add_argument("--csv", default="data/big.csv")
    args = ap.parse_args()

    buf = Path(args.parquet).read_bytes()
    footer = read_footer(buf)
    row = args.row if args.row is not None else random.Random(args.seed).randrange(footer.num_rows)
    if not 0 <= row < footer.num_rows:
        sys.exit(f"error: row must be in [0, {footer.num_rows})")

    print(f"Row:     {row}" + ("" if args.row is not None else f"  (random, seed {args.seed})"))
    print(f"CSV:     {args.csv}")
    print(f"Parquet: {args.parquet}")

    # CSV ----------------------------------------------------------------------
    header, line_off, line, fields, csv_size = csv_locate(args.csv, row)
    section("CSV")
    print(f"  Line {row + 2} (line 1 is the header), offset {line_off} ({line_off:#x}), "
          f"{len(line)} bytes plus newline")
    print(f"  Finding the line requires scanning {line_off} bytes for {row + 1} newline characters.")
    print()
    print(f"  {'Column':<12}{'Offset':>12}{'Bytes':>7}  Text")
    for name, (off, raw) in zip(header, fields):
        print(f"  {name:<12}{off:>12}{len(raw):>7}  {raw.decode()}")

    # Parquet --------------------------------------------------------------------
    g = next(i for i, rg in enumerate(footer.row_groups)
             if row < sum(r.get(3) for r in footer.row_groups[:i + 1]))
    refs = [c for c in footer.chunks if c.row_group == g]
    local = row - refs[0].row_start
    codec = CODEC.get(refs[0].meta.get(4))
    section("Parquet")
    print(f"  Row group {g} (rows {refs[0].row_start}-{refs[0].row_start + refs[0].row_count - 1}), "
          f"local row {local}, codec {codec}")
    print("  Offsets marked + are relative to the decompressed page body.")
    print()
    print(f"  {'Column':<12}{'Page':>5}{'Page offset':>13}  {'Encoding':<16}{'Def':>4}{'Index':>8}"
          f"  {'Value bytes':<28}Value")
    found = [parquet_locate(buf, ref, local) for ref in refs]
    for f in found:
        idx = "-" if f["index"] is None else str(f["index"])
        raw = f["raw"][:8].hex(" ") + (" .." if len(f["raw"]) > 8 else "")
        print(f"  {f['column']:<12}{f['page']:>5}{f['page_offset']:>13}  {f['encoding']:<16}"
              f"{f['def']:>4}{idx:>8}  {raw:<28}{fmt(f['value'])}")
    print()
    print("  Storage location of each value:")
    for f in found:
        print(f"    {f['column']:<12}{f['stored']}")

    # Verification ---------------------------------------------------------------
    import pyarrow.parquet as pq
    expected = pq.ParquetFile(args.parquet).read_row_group(g).slice(local, 1).to_pylist()[0]
    mismatches = [f["column"] for f in found if f["value"] != expected[f["column"]]]
    section("Verification")
    if mismatches:
        print(f"  FAILED for: {', '.join(mismatches)}")
    else:
        print(f"  All {len(found)} decoded values match PyArrow.")

    # Access cost ----------------------------------------------------------------
    pages = sum(f["header_bytes"] + f["page_bytes"] for f in found)
    dicts = sum(f["dict_bytes"] for f in found)
    n_dict = sum(1 for f in found if f["dict_bytes"])
    access = [("one row", len(line) + 1, pages + dicts)]
    section("Bytes read")
    print(f"  {'Access pattern':<44}{'CSV':>14}{'Parquet':>14}")
    print(f"  {'This row (after it has been located)':<44}{len(line) + 1:>14}{pages + dicts:>14}")
    print(f"  {'  of which data pages':<44}{'':>14}{pages:>14}")
    print(f"  {'  of which dictionary pages (' + str(n_dict) + ')':<44}{'':>14}{dicts:>14}")
    for name in ("price", "category"):
        col_bytes = sum(c.meta.get(7) for c in footer.chunks if c.schema_element.get(4).decode() == name)
        print(f"  {'Column ' + name + ', all rows':<44}{csv_size:>14}{col_bytes + footer.length + 8:>14}")
        access.append((f"column {name}", csv_size, col_bytes + footer.length + 8))
    print()
    print("  CSV reads for a column include every byte of the file. Parquet reads for a")
    print("  column include that column's chunks plus the footer and trailer.")

    import csv
    with open(ACCESS_OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["access", "row", "csv_bytes", "parquet_bytes"])
        for name, c_b, p_b in access:
            w.writerow([name, row, c_b, p_b])
    print(f"\nwrote {ACCESS_OUT}")


if __name__ == "__main__":
    main()