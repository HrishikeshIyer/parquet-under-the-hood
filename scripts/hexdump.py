#!/usr/bin/env python3
"""A tiny `xxd`-equivalent, so readers can reproduce the dumps without xxd/od.

    python scripts/hexdump.py <file> [offset] [length]

Matches `xxd -s <offset> -l <length> <file>`: 16 bytes per line, 8-bit hex in
space-separated pairs, printable ASCII on the right (non-printable shown as '.').
Offset and length accept decimal or 0x-hex.
"""
import sys


def _int(s: str) -> int:
    return int(s, 0)  # 0x.. or decimal


def hexdump(data: bytes, base: int = 0, width: int = 16) -> str:
    out = []
    for i in range(0, len(data), width):
        chunk = data[i:i + width]
        hex_pairs = " ".join(f"{b:02x}" for b in chunk)
        hex_pairs = f"{hex_pairs:<{width * 3 - 1}}"
        ascii_col = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        out.append(f"{base + i:08x}: {hex_pairs}  {ascii_col}")
    return "\n".join(out)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    path = sys.argv[1]
    offset = _int(sys.argv[2]) if len(sys.argv) > 2 else 0
    length = _int(sys.argv[3]) if len(sys.argv) > 3 else 256
    with open(path, "rb") as f:
        f.seek(offset)
        data = f.read(length)
    print(hexdump(data, base=offset))


if __name__ == "__main__":
    main()