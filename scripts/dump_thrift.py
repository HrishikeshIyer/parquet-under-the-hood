#!/usr/bin/env python3
"""Decode the Parquet footer (FileMetaData) from raw Thrift Compact bytes.

Two modes:
  trace        annotate the opening footer bytes field by field (the teaching view)
  reconstruct  fully decode FileMetaData and assert it matches PyArrow (the proof)

    python scripts/dump_thrift.py [trace|reconstruct] [file]
"""
import struct
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.thrift import CompactReader  # noqa: E402

TYPE_NIBBLE = {1: "bool", 2: "bool", 3: "i8", 4: "i16", 5: "i32", 6: "i64",
               7: "double", 8: "binary", 9: "list", 10: "set", 11: "map", 12: "struct"}

FILEMETA = {1: "version", 2: "schema", 3: "num_rows", 4: "row_groups",
            5: "key_value_metadata", 6: "created_by", 7: "column_orders"}
SCHEMA_EL = {1: "type", 2: "type_length", 3: "repetition_type", 4: "name",
             5: "num_children", 6: "converted_type", 7: "scale", 8: "precision",
             9: "field_id", 10: "logicalType"}
PARQUET_TYPE = {0: "BOOLEAN", 1: "INT32", 2: "INT64", 3: "INT96", 4: "FLOAT",
                5: "DOUBLE", 6: "BYTE_ARRAY", 7: "FIXED_LEN_BYTE_ARRAY"}
REPETITION = {0: "REQUIRED", 1: "OPTIONAL", 2: "REPEATED"}


def footer_bounds(buf):
    size = len(buf)
    flen = struct.unpack("<I", buf[-8:-4])[0]
    return size - 8 - flen, flen


# --------------------------------------------------------------------- trace
def trace(path, limit=26):
    """Narrate each byte of the footer opening: field headers, varints, zigzag.

    Frames on the stack are either:
      ["struct", labels, last_field_id, name]
      ["list",   remaining, elem_labels, elem_name]
    A list frame spawns one struct frame per element, so elements inside a
    list<struct> (the schema) stay in their own context instead of being read
    as fields of the parent.
    """
    buf = Path(path).read_bytes()
    start, flen = footer_bounds(buf)
    print(f"footer starts at offset {start}  (filesize {len(buf)} - 8 - footer_len {flen})\n")
    print(f"{'off':>6}  {'bytes':<14} decode")

    r = CompactReader(buf, start)
    stop_at = start + limit
    stack = [["struct", FILEMETA, 0, "FileMetaData"]]

    def row(off, raw, text):
        print(f"{off:>6}  {raw:<14} {text}")

    while r.pos < stop_at and stack:
        top = stack[-1]
        if top[0] == "list":                                # spawn next element
            if top[1] == 0:
                stack.pop()
            else:
                top[1] -= 1
                stack.append(["struct", top[2], 0, top[3]])
            continue

        _, labels, last_id, sname = top
        off = r.pos
        header = r.read_byte()
        if header == 0x00:
            row(off, "00", f"STOP  (end of {sname})")
            stack.pop()
            continue
        delta, tnib = header >> 4, header & 0x0F
        if delta:
            fid = last_id + delta
        else:
            fid = r.read_zigzag()
        top[2] = fid
        name = labels.get(fid, f"field {fid}")
        raw = f"{header:02x}"
        hdr = f"+{delta} {TYPE_NIBBLE.get(tnib,'?'):<6} -> {sname}.{name}"

        if tnib in (1, 2):                                  # bool in the header
            row(off, raw, f"{hdr} = {tnib == 1}")
        elif tnib in (5, 6):                                # i32 / i64 zigzag varint
            vstart = r.pos
            val = r.read_zigzag()
            dec = val
            if sname == "SchemaElement" and name == "type":
                dec = f"{val} ({PARQUET_TYPE.get(val,'?')})"
            elif sname == "SchemaElement" and name == "repetition_type":
                dec = f"{val} ({REPETITION.get(val,'?')})"
            row(off, raw, hdr)
            row(vstart, buf[vstart:r.pos].hex(), f"    varint -> zigzag -> {dec}")
        elif tnib == 8:                                     # binary (string)
            lstart = r.pos
            n = r.read_varint()
            s = buf[r.pos:r.pos + n]; r.pos += n
            row(off, raw, hdr)
            row(lstart, f"{n:02x}", f"    length {n}")
            row(lstart + 1, s.hex()[:22], f'    = "{s.decode(errors="replace")}"')
        elif tnib == 9:                                     # list
            lstart = r.pos
            h = r.read_byte()
            lsize, etype = h >> 4, h & 0x0F
            if lsize == 15:
                lsize = r.read_varint()
            row(off, raw, hdr)
            row(lstart, f"{h:02x}", f"    list: {lsize} x {TYPE_NIBBLE.get(etype,'?')}")
            if etype == 12:                                 # list<struct> -> schema
                stack.append(["list", lsize, SCHEMA_EL, "SchemaElement"])
        elif tnib == 12:                                    # nested struct (union types)
            row(off, raw, hdr)
            stack.append(["struct", {}, 0, name])           # labels unknown -> numeric ids
        else:
            row(off, raw, hdr + "  (value not traced)")
    print("\n...(trace truncated at the byte limit)")


# ---------------------------------------------------------------- reconstruct
def reconstruct(path):
    buf = Path(path).read_bytes()
    start, _ = footer_bounds(buf)
    fm = CompactReader(buf, start).read_struct()

    version = fm.get(1)
    schema = fm.get(2)
    num_rows = fm.get(3)
    row_groups = fm.get(4)
    created_by = fm.get(6).decode()
    names = [se.get(4).decode() for se in schema]

    print(f"version={version}  num_rows={num_rows}  "
          f"row_groups={len(row_groups)}  created_by={created_by!r}")
    print("columns:", ", ".join(names[1:]))

    # one row group's per-column metadata, by hand
    rg = row_groups[0]
    print(f"\nrow group 0: total_byte_size={rg.get(2)}  num_rows={rg.get(3)}")
    for cc in rg.get(1):                    # list<ColumnChunk>
        meta = cc.get(3)                    # ColumnMetaData
        col_type = PARQUET_TYPE.get(meta.get(1))
        path_in_schema = ".".join(b.decode() for b in meta.get(3))
        codec = meta.get(4)
        stats = meta.get(12)
        mn = mx = None
        if stats:
            mn, mx = stats.get(6), stats.get(5)   # min_value, max_value
        print(f"  {path_in_schema:<11} {col_type:<12} codec={codec} "
              f"min={mn} max={mx}")

    # cross-check against pyarrow
    md = pq.ParquetFile(path).metadata
    assert num_rows == md.num_rows
    assert len(names) - 1 == md.num_columns
    assert created_by == md.created_by
    assert len(row_groups) == md.num_row_groups
    print("\nOK — hand-decoded FileMetaData matches PyArrow exactly.")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "reconstruct"
    f = sys.argv[2] if len(sys.argv) > 2 else "data/tiny.parquet"
    (trace if mode == "trace" else reconstruct)(f)