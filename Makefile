.PHONY: setup tiny dump bench plots all clean
	big pages sample plots-part2 structure-big

setup:
	uv sync

tiny:
	uv run python scripts/gen_tiny.py

dump: tiny
	uv run python scripts/dump_footer.py data/tiny.parquet

encoded:
	uv run python scripts/gen_tiny_encoded_compressed.py

contrast: tiny encoded
	uv run python scripts/show_compression.py

decode-page: tiny
	uv run python scripts/decode_page.py category

trace-footer: tiny
	uv run python scripts/dump_thrift.py trace data/tiny.parquet

decode-footer: tiny
	uv run python scripts/dump_thrift.py reconstruct data/tiny.parquet

structure: tiny
	uv run python scripts/structure.py data/tiny.parquet $(ARGS)

structure-compressed: encoded
	uv run python scripts/structure.py data/tiny_encoded_compressed.parquet $(ARGS)
data/big.parquet: scripts/gen_big.py src/pqlab/data.py
	uv run python scripts/gen_big.py

big: data/big.parquet

pages: data/big.parquet
	uv run python scripts/page_stats.py data/big.parquet

sample: data/big.parquet
	uv run python scripts/sample_row.py $(ARGS)

plots-part2: pages sample
	uv run python scripts/plot_part2.py

structure-big: data/big.parquet
	uv run python scripts/structure.py data/big.parquet --row-group 0 --pages 2 --max 4 $(ARGS)

bench:
	uv run python scripts/bench_sizes.py
	uv run python scripts/bench_rowgroups.py
	uv run python scripts/bench_compression.py
	uv run python scripts/bench_columns.py

plots: bench
	uv run python scripts/plot.py

all: dump plots

clean:
	rm -f data/results*.csv figures/*.png