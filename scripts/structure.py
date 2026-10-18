#!/usr/bin/env python3
"""Print the physical structure of a Parquet file, decoded byte by byte.

usage: structure.py FILE [--column NAME] [--row-group N] [--max N] [--pages N]
                         [--no-verify]

Sections, in file-reading order:
  Header            leading magic
  Trailer           footer length and trailing magic
  Footer location   derived offsets of the column data and the footer
  File metadata     FileMetaData fields, schema, key/value metadata, row groups
  Column chunks     per chunk: dictionary page, then data pages; each page
                    header field by field, then the decoded page body
  Size breakdown    bytes per region

Each byte-level line has four columns: decimal offset, hexadecimal offset,
raw bytes, and description. Offsets inside a decompressed page body are
relative to the start of that body and are prefixed with '+'.

Decoding uses pqlab.thrift (Thrift Compact Protocol) and the encoders in this
file. PyArrow is used only to verify each decoded column chunk; pass
--no-verify to skip verification. Supported: flat schemas, V1 data pages,
PLAIN and dictionary encodings.
"""
import argparse
import contextlib
import datetime as dt
import io
import struct
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.thrift import CT_LIST, CT_SET, CT_STRUCT, CompactReader, Field, Struct  # noqa: E402

# ------------------------------------------------------------------ enums
PAGE_TYPE = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}
ENCODING = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED",
            5: "DELTA_BINARY_PACKED", 6: "DELTA_LENGTH_BYTE_ARRAY",
            7: "DELTA_BYTE_ARRAY", 8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}
CODEC = {0: "UNCOMPRESSED", 1: "SNAPPY", 2: "GZIP", 3: "LZO", 4: "BROTLI",
         5: "LZ4", 6: "ZSTD", 7: "LZ4_RAW"}
PHYS = {0: "BOOLEAN", 1: "INT32", 2: "INT64", 3: "INT96", 4: "FLOAT",
        5: "DOUBLE", 6: "BYTE_ARRAY", 7: "FIXED_LEN_BYTE_ARRAY"}
REPETITION = {0: "REQUIRED", 1: "OPTIONAL", 2: "REPEATED"}
LOGICAL_NAME = {"utf8": "STRING", "decimal": "DECIMAL", "date": "DATE",
                "ts_ms": "TIMESTAMP(MILLIS)", "ts_us": "TIMESTAMP(MICROS)",
                "ts_ns": "TIMESTAMP(NANOS)"}
PLAIN_LAYOUT = {0: "1 bit per value, LSB first",
                1: "int32, little-endian",
                2: "int64, little-endian",
                3: "12 bytes per value",
                4: "IEEE 754 binary32, little-endian",
                5: "IEEE 754 binary64, little-endian",
                6: "uint32 length + bytes"}

FILEMETA_F = {1: "version", 2: "schema", 3: "num_rows", 4: "row_groups",
              5: "key_value_metadata", 6: "created_by", 7: "column_orders"}
LIST_ELEM = {"schema": "SchemaElement", "row_groups": "RowGroup",
             "key_value_metadata": "KeyValue", "column_orders": "ColumnOrder"}
PAGEHDR_F = {1: "type", 2: "uncompressed_page_size", 3: "compressed_page_size",
             4: "crc", 5: "data_page_header", 6: "index_page_header",
             7: "dictionary_page_header", 8: "data_page_header_v2"}
DATAPAGE_F = {1: "num_values", 2: "encoding", 3: "definition_level_encoding",
              4: "repetition_level_encoding", 5: "statistics"}
DICTPAGE_F = {1: "num_values", 2: "encoding", 3: "is_sorted"}
STATS_F = {1: "max", 2: "min", 3: "null_count", 4: "distinct_count",
           5: "max_value", 6: "min_value", 7: "is_max_value_exact",
           8: "is_min_value_exact"}
SUBSTRUCT = {"data_page_header": DATAPAGE_F, "dictionary_page_header": DICTPAGE_F,
             "statistics": STATS_F}

FIXED = {1: ("<i", 4), 2: ("<q", 8), 4: ("<f", 4), 5: ("<d", 8)}
EPOCH = dt.datetime(1970, 1, 1)
EPOCH_DATE = dt.date(1970, 1, 1)

# ------------------------------------------------------------------ output
HEX_BYTES = 12
HEX_W = HEX_BYTES * 3 + 2
NAME_W = 30
LEFT = 2 + 8 + 2 + 8 + 2 + HEX_W + 2          # width of the offset/hex columns


def _offsets(off) -> str:
    if isinstance(off, int):
        return f"{off:>8}  {off:#08x}"
    rel = int(off)                               # relative to a decompressed body
    return f"{'+' + str(rel):>8}  {'+' + format(rel, '#07x'):>8}"


def line(off, raw: bytes, desc: str, depth: int = 0) -> None:
    hx = raw[:HEX_BYTES].hex(" ") + (" .." if len(raw) > HEX_BYTES else "")
    print(f"  {_offsets(off)}  {hx:<{HEX_W}}  {'  ' * depth}{desc}")


def field(off, raw: bytes, name: str, value: str, depth: int = 0) -> None:
    line(off, raw, f"{name:<{NAME_W - 2 * depth}}{value}".rstrip(), depth)


def detail(text: str, depth: int = 0) -> None:
    """A description with no bytes of its own, aligned to the description column."""
    print(f"{'':<{LEFT}}{'  ' * depth}{text}")


def text(s: str = "", depth: int = 1) -> None:
    print(f"{'  ' * depth}{s}")


def section(title: str) -> None:
    print(f"\n{title}\n{'=' * len(title)}")


def subsection(title: str, depth: int = 1) -> None:
    print(f"\n{'  ' * depth}{title}")


def column_header() -> None:
    print(f"  {'Offset':>8}  {'Hex':>8}  {'Bytes':<{HEX_W}}  Description")


def fmt(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (bytes, bytearray)):
        return "0x" + bytes(v).hex()
    if isinstance(v, str):
        return f'"{v}"'
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (dt.date, Decimal)):
        return str(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    return repr(v)


# ------------------------------------------------------------------ column model
class Column:
    """A leaf column: schema element (type information) and chunk metadata (location)."""

    def __init__(self, se, md=None):
        self.name = se.get(4).decode()
        self.phys = se.get(1)
        self.type_length = se.get(2)
        self.rep = se.get(3, 0)
        if self.rep == 2:
            sys.exit(f"error: column '{self.name}' is REPEATED; nested schemas are not supported")
        self.max_def = 1 if self.rep == 1 else 0
        self.scale = se.get(7) or 0
        self.kind = self._logical_kind(se.get(6), se.get(10))
        if md is not None:
            self.encodings = [ENCODING.get(e, str(e)) for e in md.get(2)]
            self.codec = md.get(4)
            self.num_values = md.get(5)
            self.usize, self.csize = md.get(6), md.get(7)
            self.data_off, self.dict_off = md.get(9), md.get(11)
            self.stats = md.get(12)
            self.start = self.dict_off if self.dict_off else self.data_off

    def _logical_kind(self, converted, logical):
        by_converted = {0: "utf8", 5: "decimal", 6: "date", 9: "ts_ms", 10: "ts_us"}
        if converted in by_converted:
            return by_converted[converted]
        if logical is not None and logical.fields:
            f = logical.fields[0]
            if f.id == 1:
                return "utf8"
            if f.id == 5:
                self.scale = f.value.get(1) or 0
                return "decimal"
            if f.id == 6:
                return "date"
            if f.id == 8:
                return {1: "ts_ms", 2: "ts_us", 3: "ts_ns"}[f.value.get(2).fields[0].id]
        return None

    @property
    def physical_label(self) -> str:
        t = PHYS[self.phys]
        return f"{t}({self.type_length})" if self.phys == 7 else t

    @property
    def logical_label(self) -> str:
        if not self.kind:
            return "-"
        name = LOGICAL_NAME[self.kind]
        return f"{name}(scale={self.scale})" if self.kind == "decimal" else name

    def logical(self, v):
        if v is None:
            return None
        k = self.kind
        if k == "utf8":
            return bytes(v).decode("utf-8", errors="replace")
        if k == "date":
            return EPOCH_DATE + dt.timedelta(days=v)
        if k == "ts_us":
            return EPOCH + dt.timedelta(microseconds=v)
        if k == "ts_ms":
            return EPOCH + dt.timedelta(milliseconds=v)
        if k == "ts_ns":
            return EPOCH + dt.timedelta(microseconds=v // 1000)
        if k == "decimal":
            iv = v if isinstance(v, int) else int.from_bytes(v, "big", signed=True)
            return Decimal(iv).scaleb(-self.scale)
        if isinstance(v, (bytes, bytearray)):
            return bytes(v)
        return v

    def from_stat(self, raw):
        """Statistics hold min/max as raw physical bytes, without a length prefix."""
        if raw is None:
            return None
        if self.phys == 0:
            return bool(raw[0])
        if self.phys in FIXED:
            return struct.unpack(FIXED[self.phys][0], raw)[0]
        return bytes(raw)

    def plain_layout(self) -> str:
        if self.phys == 7:
            return f"{self.type_length} bytes per value"
        if self.kind == "decimal" and self.phys == 7:
            return f"{self.type_length}-byte big-endian two's complement"
        return PLAIN_LAYOUT[self.phys]


# ------------------------------------------------------------------ decoders
def read_plain(col, data, p, count, at):
    """Decode `count` PLAIN values. Returns ([(offset, raw, physical_value)], end)."""
    out = []
    if col.phys == 0:
        for i in range(count):
            q = p + i // 8
            out.append((at(q), data[q:q + 1], bool((data[q] >> (i % 8)) & 1)))
        return out, p + (count + 7) // 8
    if col.phys in FIXED:
        fmt_, n = FIXED[col.phys]
        for _ in range(count):
            out.append((at(p), data[p:p + n], struct.unpack_from(fmt_, data, p)[0]))
            p += n
        return out, p
    if col.phys == 6:
        for _ in range(count):
            n = struct.unpack_from("<i", data, p)[0]
            out.append((at(p), data[p:p + 4 + n], data[p + 4:p + 4 + n]))
            p += 4 + n
        return out, p
    n = col.type_length if col.phys == 7 else 12
    for _ in range(count):
        out.append((at(p), data[p:p + n], data[p:p + n]))
        p += n
    return out, p


def hybrid_runs(data, bit_width, total, at):
    """Decode the RLE / bit-packing hybrid, keeping each run's bytes."""
    runs, got, p = [], 0, 0
    width_bytes = (bit_width + 7) // 8
    mask = (1 << bit_width) - 1
    while got < total and p < len(data):
        start = p
        header, shift = 0, 0
        while True:
            b = data[p]
            p += 1
            header |= (b & 0x7F) << shift
            if not b & 0x80:
                break
            shift += 7
        hdr_end = p
        if header & 1:
            groups = header >> 1
            nbytes = groups * bit_width
            bits = int.from_bytes(data[p:p + nbytes], "little")
            p += nbytes
            vals = [(bits >> (i * bit_width)) & mask for i in range(groups * 8)]
            kind = "bit-packed"
        else:
            vals = [int.from_bytes(data[p:p + width_bytes], "little")] * (header >> 1)
            p += width_bytes
            kind = "RLE"
        runs.append({"at": at(start), "hdr": data[start:hdr_end], "body": data[hdr_end:p],
                     "header": header, "kind": kind, "vals": vals})
        got += len(vals)
    return runs, [v for r in runs for v in r["vals"]][:total]


def print_runs(runs, bit_width, limit, depth):
    for r in runs[:limit]:
        n = len(r["vals"])
        if r["kind"] == "RLE":
            desc = f"RLE run (header {r['header']}): value {r['vals'][0]} repeated {n} times"
        else:
            preview = " ".join(map(str, r["vals"][:12])) + (" ..." if n > 12 else "")
            desc = (f"bit-packed run (header {r['header']}): {r['header'] >> 1} x 8 values, "
                    f"width {bit_width}: {preview}")
        line(r["at"], r["hdr"] + r["body"], desc, depth)
    if len(runs) > limit:
        detail(f"({len(runs) - limit} further runs)", depth)


def decompress(codec, raw, usize):
    import cramjam
    if codec == 1:
        return bytes(cramjam.snappy.decompress_raw(raw))
    if codec == 2:
        return bytes(cramjam.gzip.decompress(raw))
    if codec == 4:
        return bytes(cramjam.brotli.decompress(raw))
    if codec == 6:
        return bytes(cramjam.zstd.decompress(raw))
    if codec == 7:
        return bytes(cramjam.lz4.decompress_block(raw, output_len=usize))
    sys.exit(f"error: codec {CODEC.get(codec, codec)} is not supported")


# ------------------------------------------------------------------ Thrift
def describe(name, v, col):
    if name == "type":
        return f"{v} ({PAGE_TYPE.get(v, 'unknown')})"
    if name.endswith("encoding"):
        return f"{v} ({ENCODING.get(v, 'unknown')})"
    if name in ("min_value", "max_value", "min", "max"):
        return fmt(col.logical(col.from_stat(v)))
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (bytes, bytearray)):
        return "0x" + bytes(v).hex()
    return str(v)


def print_struct(buf, st, names, stop_at, depth, col):
    for f in st.fields:
        name = names.get(f.id, f"field_{f.id}")
        if f.type == CT_STRUCT:
            field(f.start, buf[f.start:f.start + 1], name, "struct", depth)
            print_struct(buf, f.value, SUBSTRUCT.get(name, {}), f.end - 1, depth + 1, col)
        else:
            field(f.start, buf[f.start:f.end], name, describe(name, f.value, col), depth)
    field(stop_at, buf[stop_at:stop_at + 1], "(stop)", "", depth)


# ------------------------------------------------------------------ pages
def dictionary_page(col, dph, data, p, at, args):
    n, enc = dph.get(1), dph.get(2)
    subsection(f"Dictionary: {n} entries, {ENCODING.get(enc)} ({col.plain_layout()})", 3)
    entries, _ = read_plain(col, data, p, n, at)
    for i, (o, raw, v) in enumerate(entries[:args.max]):
        length = f"len {len(v):<4}" if col.phys == 6 else ""
        line(o, raw, f"[{i}]{'':<{6 - len(str(i))}}{length}{fmt(col.logical(v))}")
    if n > args.max:
        detail(f"({n - args.max} further entries)")
    return [v for _, _, v in entries]


def data_page(col, dp, data, p, end, at, dictionary, args):
    num, enc = dp.get(1), dp.get(2)

    if col.max_def > 0:
        n_def = struct.unpack_from("<I", data, p)[0]
        subsection(f"Definition levels: {n_def} bytes, RLE/bit-packed hybrid, bit width 1", 3)
        field(at(p), data[p:p + 4], "length", f"{n_def}")
        p += 4
        runs, defs = hybrid_runs(data[p:p + n_def], 1, num, lambda q, b=p: at(b + q))
        print_runs(runs, 1, args.max, 0)
        nulls = defs.count(0)
        detail(f"{num} levels: {num - nulls} defined, {nulls} null")
        p += n_def
    else:
        defs = [0] * num
        subsection("Definition levels: none (REQUIRED column)", 3)
    present = sum(1 for d in defs if d == col.max_def)

    idxs = None
    if enc in (2, 8):
        bw = data[p]
        runs, idxs = hybrid_runs(data[p + 1:end], bw, present, lambda q, b=p + 1: at(b + q))
        top = max(idxs) if idxs else 0
        subsection(f"Values: {present} dictionary indices, {ENCODING[enc]}", 3)
        field(at(p), data[p:p + 1], "bit width", f"{bw} (max index on page: {top})")
        print_runs(runs, bw, args.max, 0)
        phys_vals = [dictionary[i] for i in idxs]
    elif enc == 0:
        subsection(f"Values: {present} x PLAIN ({col.plain_layout()})", 3)
        entries, _ = read_plain(col, data, p, present, at)
        shown = entries[:args.max]
        if col.phys == 0:
            for k in range(0, len(shown), 8):
                o, raw, _ = shown[k]
                bits = " ".join("1" if v else "0" for _, _, v in entries[k:k + 8])
                line(o, raw, f"bits {bits}")
        else:
            for o, raw, v in shown:
                line(o, raw, fmt(col.logical(v)))
        if present > args.max:
            detail(f"({present - args.max} further values)")
        phys_vals = [v for _, _, v in entries]
    else:
        subsection(f"Values: encoding {ENCODING.get(enc, enc)} is not decoded", 3)
        return [None] * num

    values = iter(phys_vals)
    index_iter = iter(idxs) if idxs is not None else None
    out, shown_rows = [], []
    for r_i, d in enumerate(defs):
        if d == col.max_def:
            v = col.logical(next(values))
            ix = next(index_iter) if index_iter else None
            out.append(v)
        else:
            v, ix = None, None
            out.append(None)
        if r_i < args.max:
            shown_rows.append((r_i, d, ix, v))

    subsection(f"Rows (first {len(shown_rows)} of {num})", 3)
    hdr = f"{'row':>6}  {'def':>3}  {'index':>6}  value" if idxs is not None else f"{'row':>6}  {'def':>3}  value"
    detail(hdr)
    for r_i, d, ix, v in shown_rows:
        if idxs is not None:
            detail(f"{r_i:>6}  {d:>3}  {'-' if ix is None else ix:>6}  {fmt(v)}")
        else:
            detail(f"{r_i:>6}  {d:>3}  {fmt(v)}")
    return out


def page(buf, col, pos, page_no, dictionary, values, args):
    r = CompactReader(buf, pos)
    ph = r.read_struct()
    hdr_end = r.pos
    ptype, usize, csize = ph.get(1), ph.get(2), ph.get(3)
    subsection(f"Page {page_no}: {PAGE_TYPE.get(ptype, 'UNKNOWN')} at offset {pos}, "
               f"header {hdr_end - pos} bytes, body {csize} bytes", 2)
    subsection("Page header (Thrift Compact)", 3)
    print_struct(buf, ph, PAGEHDR_F, hdr_end - 1, 0, col)

    body = buf[hdr_end:hdr_end + csize]
    if col.codec:
        data = decompress(col.codec, body, usize)
        subsection(f"Page body: {csize} bytes {CODEC[col.codec]}, {len(data)} bytes decompressed", 3)
        line(hdr_end, body, f"compressed body ({csize} bytes)")
        detail("offsets below are relative to the decompressed body")
        start, end, at = 0, len(data), (lambda q: f"{q}")
    else:
        data, start, end, at = buf, hdr_end, hdr_end + csize, (lambda q: q)

    sub = ph.get(7) if ptype == 2 else ph.get(5)
    summary = (page_no, pos, PAGE_TYPE.get(ptype, "UNKNOWN"),
               ENCODING.get(sub.get(2), "-") if sub else "-",
               sub.get(1) if sub else 0, csize, usize)
    if ptype == 2:
        dictionary = dictionary_page(col, sub, data, start, at, args)
    elif ptype == 0:
        values += data_page(col, sub, data, start, end, at, dictionary, args)
    else:
        subsection(f"Page body: {PAGE_TYPE.get(ptype)} is not decoded", 3)
    return hdr_end + csize, dictionary, summary


def column_chunk(buf, col, n, total, args, expected):
    rg = f"row group {col.row_group}, " if args.multi_rg else ""
    section(f"Column chunk {n}/{total}: {rg}{col.name}")
    logical = f" / {col.logical_label}" if col.kind else ""
    text(f"Type:       {col.physical_label}{logical}, {REPETITION[col.rep]}")
    text(f"Range:      [{col.start}, {col.start + col.csize}), {col.csize} bytes "
         f"({col.usize} bytes uncompressed)")
    text(f"Codec:      {CODEC.get(col.codec)}")
    text(f"Encodings:  {', '.join(col.encodings)}")
    print()
    column_header()

    pos, end = col.start, col.start + col.csize
    dictionary, values, page_no, hidden = None, [], 0, []
    while pos < end:
        page_no += 1
        quiet = bool(args.pages) and page_no > args.pages
        with contextlib.redirect_stdout(io.StringIO()) if quiet else contextlib.nullcontext():
            pos, dictionary, summary = page(buf, col, pos, page_no, dictionary, values, args)
        if quiet:
            hidden.append(summary)

    if hidden:
        subsection(f"Remaining pages ({len(hidden)}, summarised; use --pages to expand)", 2)
        text(f"{'Page':>6}  {'Offset':>10}  {'Type':<16}{'Encoding':<16}{'Values':>8}"
             f"{'Bytes':>10}{'Raw bytes':>11}", 3)
        for n_, o_, t_, e_, v_, c_, u_ in hidden:
            text(f"{n_:>6}  {o_:>10}  {t_:<16}{e_:<16}{v_:>8}{c_:>10}{u_:>11}", 3)

    if expected is not None:
        print()
        if len(values) != len(expected):
            text(f"Verification: FAILED (decoded {len(values)} values, PyArrow returned {len(expected)})")
        elif values == expected:
            text(f"Verification: {len(values)} of {len(expected)} values match PyArrow")
        else:
            bad = next(i for i, (a, b) in enumerate(zip(values, expected)) if a != b)
            text(f"Verification: FAILED at row {bad} "
                 f"(decoded {fmt(values[bad])}, PyArrow {fmt(expected[bad])})")


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(
        description="Print the physical structure of a Parquet file, decoded byte by byte.")
    ap.add_argument("file", help="path to a .parquet file")
    ap.add_argument("--column", action="append", metavar="NAME",
                    help="limit output to this column (repeatable)")
    ap.add_argument("--row-group", type=int, action="append", metavar="N",
                    help="limit output to this row group (repeatable)")
    ap.add_argument("--max", type=int, default=8, metavar="N",
                    help="values, runs and rows listed per page (default: 8)")
    ap.add_argument("--pages", type=int, default=0, metavar="N",
                    help="pages per chunk shown in full; the rest are summarised (default: all)")
    ap.add_argument("--no-verify", action="store_true",
                    help="skip verification of decoded values against PyArrow")
    args = ap.parse_args()

    buf = Path(args.file).read_bytes()
    size = len(buf)
    print(f"File: {args.file}")
    print(f"Size: {size} bytes")

    # Header -------------------------------------------------------------------
    section("Header")
    column_header()
    line(0, buf[:4], f'magic "{buf[:4].decode(errors="replace")}"')
    if buf[:4] != b"PAR1":
        sys.exit("error: missing PAR1 header magic; not a Parquet file")

    # Trailer ------------------------------------------------------------------
    flen = struct.unpack("<I", buf[-8:-4])[0]
    section("Trailer")
    column_header()
    field(size - 8, buf[-8:-4], "footer length", f"{flen} (uint32, little-endian)")
    field(size - 4, buf[-4:], "magic", f'"{buf[-4:].decode(errors="replace")}"')

    # Footer location ----------------------------------------------------------
    fstart = size - 8 - flen
    section("Footer location")
    text(f"footer offset = file size - 8 - footer length = {size} - 8 - {flen} = {fstart}")
    text()
    text(f"{'Region':<16}{'Start':>8}{'End':>8}{'Bytes':>8}")
    text(f"{'header':<16}{0:>8}{4:>8}{4:>8}")
    text(f"{'column data':<16}{4:>8}{fstart:>8}{fstart - 4:>8}")
    text(f"{'footer':<16}{fstart:>8}{size - 8:>8}{flen:>8}")
    text(f"{'trailer':<16}{size - 8:>8}{size:>8}{8:>8}")

    # File metadata ------------------------------------------------------------
    r = CompactReader(buf, fstart)
    fm = r.read_struct()
    section("File metadata (FileMetaData, Thrift Compact Protocol)")
    column_header()
    for f in fm.fields:
        name = FILEMETA_F.get(f.id, f"field_{f.id}")
        raw = buf[f.start:f.end]
        if f.type in (CT_LIST, CT_SET):
            count = len(f.value)
            value = (f"list<{LIST_ELEM.get(name, '?')}>, {count} element{'s' if count != 1 else ''}, "
                     f"{f.end - f.start} bytes")
        elif isinstance(f.value, (bytes, bytearray)):
            value = f'"{f.value.decode(errors="replace")}"'
        else:
            value = str(f.value)
        field(f.start, raw, name, value)
    field(r.pos - 1, buf[r.pos - 1:r.pos], "(stop)", "")

    schema = fm.get(2)
    subsection(f"Schema ({len(schema)} elements)")
    text(f"{'#':>3}  {'Name':<14}{'Physical type':<26}{'Logical type':<24}Repetition", 2)
    for i, se in enumerate(schema):
        if se.get(5) is not None:
            text(f"{i:>3}  {se.get(4).decode():<14}{'group (' + str(se.get(5)) + ' fields)':<26}"
                 f"{'-':<24}{REPETITION.get(se.get(3, 0))}", 2)
        else:
            c = Column(se)
            text(f"{i:>3}  {c.name:<14}{c.physical_label:<26}{c.logical_label:<24}"
                 f"{REPETITION[c.rep]}", 2)

    kv = fm.get(5) or []
    if kv:
        subsection("Key/value metadata")
        for item in kv:
            text(f"{item.get(1).decode():<24}{len(item.get(2) or b'')} bytes", 2)

    leaves = [se for se in schema if se.get(5) is None]
    row_groups = fm.get(4)
    chunks, row_start = [], 0
    for g, rg in enumerate(row_groups):
        subsection(f"Row group {g}: {rg.get(3)} rows, {rg.get(2)} bytes")
        text(f"{'Column':<12}{'Codec':<14}{'Dict page':>11}{'Data page':>11}{'Bytes':>9}"
             f"{'Raw bytes':>11}{'Nulls':>7}  Min / max", 2)
        for se, cc in zip(leaves, rg.get(1)):
            c = Column(se, cc.get(3))
            c.row_group, c.row_start, c.row_count = g, row_start, rg.get(3)
            nulls = c.stats.get(3) if c.stats else None
            mn = c.logical(c.from_stat(c.stats.get(6))) if c.stats else None
            mx = c.logical(c.from_stat(c.stats.get(5))) if c.stats else None
            text(f"{c.name:<12}{CODEC.get(c.codec):<14}{str(c.dict_off or '-'):>11}"
                 f"{c.data_off:>11}{c.csize:>9}{c.usize:>11}{str(nulls):>7}  "
                 f"{fmt(mn)} / {fmt(mx)}", 2)
            chunks.append(c)
        row_start += rg.get(3)

    # Column chunks --------------------------------------------------------------
    args.multi_rg = len(row_groups) > 1
    selected = chunks
    if args.row_group:
        selected = [c for c in selected if c.row_group in args.row_group]
    if args.column:
        selected = [c for c in selected if c.name in args.column]
    if not selected:
        sys.exit("error: no column chunks match the given --column / --row-group")

    table = None
    if not args.no_verify:
        import pyarrow.parquet as pq
        table = pq.read_table(args.file, columns=sorted({c.name for c in selected}))

    for n, c in enumerate(selected, 1):
        expected = None
        if table is not None:
            expected = table.column(c.name).to_pylist()[c.row_start:c.row_start + c.row_count]
        column_chunk(buf, c, n, len(selected), args, expected)

    # Size breakdown -------------------------------------------------------------
    section("Size breakdown")
    text(f"{'Region':<28}{'Bytes':>10}{'Share':>9}")
    rows = [("header", 4)]
    rows += [(f"chunk {('rg' + str(c.row_group) + ' ') if args.multi_rg else ''}{c.name}", c.csize)
             for c in chunks]
    rows += [("footer", flen), ("trailer", 8)]
    for name, b in rows:
        text(f"{name:<28}{b:>10}{100 * b / size:>8.1f}%")
    text(f"{'total':<28}{size:>10}{100.0:>8.1f}%")
    print()


if __name__ == "__main__":
    main()