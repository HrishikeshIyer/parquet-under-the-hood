# Parquet Under the Hood

This project is a byte-level teardown of the [Apache Parquet file format](https://parquet.apache.org/)]. 
There are 2 aspects to this project:
1. Crafting a richly-typed Parquet file and its CSV equivalent, then understanding the structure of each.
2. Comparing the efficiency and file sizes of the two formats by varying the number of rows. 

For detailed explanation: **[Parquet Under the Hood, Part 1](https://<yourdomain>/parquet-under-the-hood-part-1)**

## Quickstart

```bash
uv sync
make all
```

That generates the sample file, prints its full structure, runs the benchmarks,
and writes the figures to `figures/`.

Requires [`uv`](https://docs.astral.sh/uv/). Dependencies (notably `pyarrow`) are
pinned so the bytes you get match the write-up.

## What it does

| `make` target | Output |
|---|---|
| `make tiny` | `data/tiny.parquet` — uncompressed, 12 columns spanning every physical type, one row group (legible in `xxd`) |
| `make dump` | full teardown of the tiny file: magic bytes, trailer, footer length, per-column encodings + statistics |
| `make bench` | four benchmarks → `data/results*.csv` (size sweep, row-group sweep, codec sweep, per-column dictionary on/off) |
| `make plots` | seven figures → `figures/*.png` |
| `make all` | `dump` + `plots` |
| `make clean` | remove generated CSVs and figures |

## Layout

```
src/pqlab/
  data.py      # one deterministic, richly-typed table builder (shared everywhere)
  meta.py      # footer length + metadata read straight off the file trailer
scripts/
  gen_tiny.py         # the tiny, uncompressed file for the byte walkthrough
  dump_footer.py      # magic / trailer / footer / per-column metadata
  bench_sizes.py      # Parquet vs CSV across row counts; footer overhead
  bench_rowgroups.py  # fixed N, varying row-group size
  bench_compression.py# codec sweep: size, write time, read time
  bench_columns.py    # per-column size, dictionary on vs off
  plot.py             # seaborn figures from the benchmark CSVs
```

## Selected findings

Measured with `pyarrow==25.0.1`. Try it yourself with `make all`.

**Parquet wins on scale** It carries fixed metadata overhead (a ~2.2 KB footer),
so it's *larger* than CSV until a few hundred rows, then pulls away:

| rows | Parquet (snappy) | CSV | footer % of file | Parquet ÷ CSV |
|---:|---:|---:|---:|---:|
| 10 | 3,652 B | 1,051 B | 60% | **3.48×** |
| 1,000 | 41,207 B | 97,395 B | 5.5% | 0.42× |
| 1,000,000 | 25.7 MB | 100.3 MB | 0.01% | **0.26×** |

**The footer is an almost fixed overhead** — ~2.2 KB at 10 rows, ~2.4 KB at a million. That
constant is the origin of the data-lake "small files problem".

**Compression** (N = 500,000):

| codec | size | ratio | write | read |
|---|---:|---:|---:|---:|
| snappy | 16.8 MB | 2.9× | 162 ms | 110 ms |
| **zstd** | **12.2 MB** | **4.0×** | 170 ms | 89 ms |
| gzip | 11.6 MB | 4.2× | 9,515 ms | 231 ms |
| brotli | 10.7 MB | 4.6× | 1,112 ms | 161 ms |

zstd sits on the frontier — near-brotli ratio at snappy-class speed. gzip's ratio
costs a ~9.5 s write for nothing brotli doesn't beat faster.

**Dictionary encoding depends on cardinality** It shrank the 4-value `category`
column ~30×, but made high-cardinality columns (`sku`, random `payload`, sorted
`id`) slightly *larger* — dictionary overhead for no gain.

## To build it on your machine:

- `data/tiny.parquet`, the benchmark CSVs, and the figures are committed so the
  repo renders and reproduces without a run.
- The tiny file is written **uncompressed** and with a **single row group** so
  its bytes are readable in `od`/`xxd`.
- Footer length is read directly from the file trailer (last 8 bytes =
  `[len: uint32 LE][b"PAR1"]`), not via a library, so the "metadata overhead"
  numbers are honest.
