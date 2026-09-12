#!/usr/bin/env python3
'''
At fixed number of rows, compare compression codecs on file size, write time, and read time.
'''
import csv
import sys
import time
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402

N = 500_000
CODECS = ["none", "snappy", "lz4", "zstd", "gzip", "brotli"]
OUT = "data/results_compression.csv"


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    t = build_table(N)
    raw = t.nbytes  # in-memory Arrow size, as an uncompressed reference
    rows = []
    for c in CODECS:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            path = tmp.name
        t0 = time.perf_counter()
        pq.write_table(t, path, compression=c, use_dictionary=True, write_statistics=True)
        t_write = time.perf_counter() - t0
        t0 = time.perf_counter()
        pq.read_table(path)
        t_read = time.perf_counter() - t0
        size = Path(path).stat().st_size
        Path(path).unlink()
        rec = {
            "codec": c,
            "file_bytes": size,
            "ratio_vs_arrow": round(raw / size, 3),
            "write_ms": round(t_write * 1000, 1),
            "read_ms": round(t_read * 1000, 1),
        }
        rows.append(rec)
        print(rec)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()