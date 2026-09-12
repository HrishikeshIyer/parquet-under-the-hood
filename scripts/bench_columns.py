#!/usr/bin/env python3
'''
At fixed number of rows, break the file down per column, with dictionary encoding on vs off.
'''
import csv
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pqlab.data import build_table  # noqa: E402

N = 500_000
OUT = "data/results_columns.csv"


def column_sizes(table, use_dictionary) -> dict[str, int]:
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = tmp.name
    pq.write_table(table, path, compression="snappy",
                   use_dictionary=use_dictionary, write_statistics=True)
    md = pq.ParquetFile(path).metadata
    sizes: dict[str, int] = {}
    for rg in range(md.num_row_groups):
        rgmd = md.row_group(rg)
        for c in range(rgmd.num_columns):
            col = rgmd.column(c)
            sizes[col.path_in_schema] = sizes.get(col.path_in_schema, 0) + col.total_compressed_size
    Path(path).unlink()
    return sizes


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    t = build_table(N)
    phys = {f.name: str(t.schema.field(f.name).type) for f in t.schema}
    on = column_sizes(t, True)
    off = column_sizes(t, False)
    rows = []
    for name in t.schema.names:
        rows.append({
            "column": name,
            "arrow_type": phys[name],
            "dict_on_bytes": on[name],
            "dict_off_bytes": off[name],
        })
        print(rows[-1])
    rows.sort(key=lambda r: r["dict_on_bytes"], reverse=True)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()