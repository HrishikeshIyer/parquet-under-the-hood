"""Parquet value encodings decoded: PLAIN dictionaries and the
RLE / bit-packing hybrid used for dictionary indices and definition levels.
"""
from __future__ import annotations

import struct


def read_plain_byte_array(buf: bytes, pos: int, count: int) -> tuple[list[bytes], int]:
    """PLAIN BYTE_ARRAY: each value is a 4-byte LE length followed by its bytes."""
    out = []
    for _ in range(count):
        n = struct.unpack_from("<i", buf, pos)[0]
        pos += 4
        out.append(buf[pos:pos + n])
        pos += n
    return out, pos


def rle_bitpack_hybrid(data: bytes, bit_width: int, total: int) -> list[int]:
    """Decode Parquet's RLE / bit-packing hybrid into `total` integers.

    The stream is a series of runs, each introduced by a varint header:
      header & 1 == 1  -> bit-packed run of (header >> 1) groups of 8 values
      header & 1 == 0  -> RLE run of (header >> 1) copies of one value
    Bit-packed values are LSB-first.
    """
    vals: list[int] = []
    p = 0
    byte_width = (bit_width + 7) // 8
    mask = (1 << bit_width) - 1
    while len(vals) < total:
        header, shift = 0, 0
        while True:                              # varint run header
            b = data[p]; p += 1
            header |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        if header & 1:                            # bit-packed run
            count = (header >> 1) * 8
            nbytes = (header >> 1) * bit_width
            bitbuf = int.from_bytes(data[p:p + nbytes], "little"); p += nbytes
            for i in range(count):
                vals.append((bitbuf >> (i * bit_width)) & mask)
        else:                                     # RLE run
            run = header >> 1
            val = int.from_bytes(data[p:p + byte_width], "little"); p += byte_width
            vals.extend([val] * run)
    return vals[:total]