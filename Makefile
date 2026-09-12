.PHONY: setup tiny dump bench plots all clean

setup:
	uv sync

tiny:
	uv run python scripts/gen_tiny.py

dump: tiny
	uv run python scripts/dump_footer.py data/tiny.parquet

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