"""A from-scratch reader for Apache Thrift's Compact Protocol.

Parquet serializes its footer (FileMetaData) and every page header with Thrift's
Compact Protocol. This module decodes those bytes by hand — varints, ZigZag,
delta-coded field headers, struct/list framing — with zero Thrift library and no
PyArrow decode. It returns a plain tree of (field-id -> value) so the footer and
page headers can be walked and annotated byte by byte.

Compact Protocol, in one screen:
  * unsigned varint (LEB128): 7 bits per byte, little-endian, high bit = "more".
  * signed ints are ZigZag-mapped, then varint-encoded.
  * a struct is a run of fields ending in a STOP byte (0x00).
  * a field header byte = (field_id_delta << 4) | type_nibble.
      - delta 1..15  -> field_id = previous_id + delta   (short form)
      - delta 0      -> long form: field_id follows as a ZigZag varint
      - a bool field carries its value IN the type nibble (1=true, 2=false)
  * a list/set header byte = (size << 4) | elem_type;
      size 0..14 inline, size 15 -> real size follows as a varint.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# compact type nibbles
CT_STOP = 0x00
CT_BOOL_TRUE = 0x01
CT_BOOL_FALSE = 0x02
CT_BYTE = 0x03
CT_I16 = 0x04
CT_I32 = 0x05
CT_I64 = 0x06
CT_DOUBLE = 0x07
CT_BINARY = 0x08
CT_LIST = 0x09
CT_SET = 0x0A
CT_MAP = 0x0B
CT_STRUCT = 0x0C

TYPE_NAME = {
    CT_BOOL_TRUE: "bool", CT_BOOL_FALSE: "bool", CT_BYTE: "i8", CT_I16: "i16",
    CT_I32: "i32", CT_I64: "i64", CT_DOUBLE: "double", CT_BINARY: "binary",
    CT_LIST: "list", CT_SET: "set", CT_MAP: "map", CT_STRUCT: "struct",
}


@dataclass
class Field:
    """One decoded struct field, with the byte span it occupied (for annotation)."""
    id: int
    type: int
    value: object
    start: int           # offset of the field header byte
    end: int             # offset just past the field's value

    @property
    def type_name(self) -> str:
        return TYPE_NAME.get(self.type, f"0x{self.type:x}")


@dataclass
class Struct:
    fields: list = field(default_factory=list)

    def get(self, fid: int, default=None):
        for f in self.fields:
            if f.id == fid:
                return f.value
        return default

    def __repr__(self) -> str:
        return "Struct(" + ", ".join(f"{f.id}:{f.type_name}" for f in self.fields) + ")"


class CompactReader:
    def __init__(self, buf: bytes, pos: int = 0):
        self.buf = buf
        self.pos = pos

    # --- primitives ---------------------------------------------------------
    def read_byte(self) -> int:
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def read_varint(self) -> int:
        """Unsigned LEB128."""
        result = 0
        shift = 0
        while True:
            b = self.buf[self.pos]
            self.pos += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7

    @staticmethod
    def zigzag_to_int(n: int) -> int:
        return (n >> 1) ^ -(n & 1)

    def read_zigzag(self) -> int:
        return self.zigzag_to_int(self.read_varint())

    def read_double(self) -> float:
        import struct
        v = struct.unpack_from("<d", self.buf, self.pos)[0]
        self.pos += 8
        return v

    def read_binary(self) -> bytes:
        n = self.read_varint()
        v = self.buf[self.pos:self.pos + n]
        self.pos += n
        return v

    # --- value dispatch -----------------------------------------------------
    def read_value(self, type_nibble: int):
        if type_nibble == CT_BOOL_TRUE:
            return True
        if type_nibble == CT_BOOL_FALSE:
            return False
        if type_nibble == CT_BYTE:
            return self._i8()  # i8 is a single raw (signed) byte, not zigzag
        if type_nibble in (CT_I16, CT_I32, CT_I64):
            return self.read_zigzag()
        if type_nibble == CT_DOUBLE:
            return self.read_double()
        if type_nibble == CT_BINARY:
            return self.read_binary()
        if type_nibble in (CT_LIST, CT_SET):
            return self.read_list()
        if type_nibble == CT_MAP:
            return self.read_map()
        if type_nibble == CT_STRUCT:
            return self.read_struct()
        raise ValueError(f"unknown compact type 0x{type_nibble:x} at offset {self.pos}")

    def _i8(self) -> int:
        b = self.read_byte()
        return b - 256 if b >= 128 else b

    # --- containers ---------------------------------------------------------
    def read_struct(self) -> Struct:
        s = Struct()
        last_id = 0
        while True:
            start = self.pos
            header = self.read_byte()
            if header == CT_STOP:
                break
            delta = header >> 4
            type_nibble = header & 0x0F
            if delta == 0:
                fid = self.read_zigzag()
            else:
                fid = last_id + delta
            last_id = fid
            if type_nibble in (CT_BOOL_TRUE, CT_BOOL_FALSE):
                value = (type_nibble == CT_BOOL_TRUE)  # value is in the header
            else:
                value = self.read_value(type_nibble)
            s.fields.append(Field(fid, type_nibble, value, start, self.pos))
        return s

    def read_list(self) -> list:
        header = self.read_byte()
        size = header >> 4
        elem_type = header & 0x0F
        if size == 15:
            size = self.read_varint()
        return [self.read_value(elem_type) for _ in range(size)]

    def read_map(self) -> list:
        size = self.read_varint()
        if size == 0:
            return []
        kv = self.read_byte()
        key_type, val_type = kv >> 4, kv & 0x0F
        return [(self.read_value(key_type), self.read_value(val_type)) for _ in range(size)]