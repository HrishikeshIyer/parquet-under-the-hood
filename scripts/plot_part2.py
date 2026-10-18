#!/usr/bin/env python3
"""Part 2 figures, from the large-file results.

Inputs (written by page_stats.py and sample_row.py):
  data/results_pages.csv, data/results_column_bytes.csv, data/results_access.csv

Outputs:
  figures/08_page_encoding_by_column.png
  figures/09_column_size_by_stage.png
  figures/10_bit_width_vs_max_index.png
  figures/11_bytes_read_csv_vs_parquet.png
"""
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
from matplotlib.ticker import FuncFormatter, MaxNLocator  # noqa: E402

FIG = Path("figures")
FIG.mkdir(exist_ok=True)
sns.set_theme(style="whitegrid", context="talk")

# Two-category colours (passes the CVD / contrast checks together)
BLUE, RED = "#4C72B0", "#C44E52"
# Ordered stages: one hue, light to dark
STAGES = ["#A9BEDF", "#4C72B0", "#1F3560"]
INK, MUTED, GRID = "#1f2933", "#52606d", "#d9dee4"


def human_bytes(n, _pos=None) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1000 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.3g} {unit}"
        n /= 1000
    return f"{n:.3g} GB"


def style(ax):
    ax.xaxis.label.set_size(13)
    ax.yaxis.label.set_size(13)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.tick_params(colors=MUTED, labelsize=12)
    ax.xaxis.label.set_color(INK)
    ax.yaxis.label.set_color(INK)
    for s in ax.spines.values():
        s.set_visible(False)


def title(ax, text):
    ax.set_title(text, loc="left", fontsize=15, color=INK, pad=14)


# ---------------------------------------------------------------- 08
def fig_page_encoding(pages, columns):
    d = pages[(pages.row_group == 0) & (pages.type == "DATA_PAGE")].copy()
    d["start"] = d.groupby("column").num_values.cumsum() - d.num_values
    fig, ax = plt.subplots(figsize=(11, 6.2))
    for y, name in enumerate(columns):
        for _, r in d[d.column == name].iterrows():
            ax.barh(y, r.num_values, left=r.start, height=0.62,
                    color=BLUE if r.encoding == "RLE_DICTIONARY" else RED,
                    edgecolor="white", linewidth=2)
    ax.set_yticks(range(len(columns)), columns)
    ax.invert_yaxis()
    ax.set_xlim(0, d.groupby("column").num_values.sum().max())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: "0" if v == 0 else f"{v / 1000:.0f}k"))
    ax.set_xlabel("row within row group 0 (each segment is one data page)")
    style(ax)
    ax.grid(axis="y", visible=False)
    title(ax, "Data page encoding across row group 0, by column")
    ax.legend(handles=[Patch(color=BLUE, label="RLE_DICTIONARY (dictionary indices)"),
                       Patch(color=RED, label="PLAIN")],
              loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=2, frameon=False, fontsize=12)
    fig.tight_layout()
    fig.savefig(FIG / "08_page_encoding_by_column.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------- 09
def fig_column_stages(cols):
    d = cols.sort_values("arrow_bytes", ascending=True).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(11, 6.6))
    for y, r in d.iterrows():
        lo = min(r.arrow_bytes, r.encoded_bytes, r.compressed_bytes)
        hi = max(r.arrow_bytes, r.encoded_bytes, r.compressed_bytes)
        ax.plot([lo, hi], [y, y], color=GRID, linewidth=2, zorder=1)
    stages = [("arrow_bytes", "in memory (Arrow)"),
              ("encoded_bytes", "after encoding (uncompressed pages)"),
              ("compressed_bytes", "after compression (ZSTD, stored)")]
    for (col, label), colour, dy in zip(stages, STAGES, (-0.22, 0.0, 0.22)):
        ax.scatter(d[col], d.index + dy, s=80, color=colour, edgecolor="white",
                   linewidth=1.5, zorder=3, label=label)
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.set_yticks(d.index, d.column)
    ax.set_xlabel("bytes (log scale)")
    style(ax)
    title(ax, "Column size in memory, after encoding, and after compression")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3, frameon=False,
              fontsize=11, handletextpad=0.3, columnspacing=1.2)
    fig.tight_layout()
    fig.savefig(FIG / "09_column_size_by_stage.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------- 10
def fig_bit_width(pages):
    d = pages[pages.max_index.notna()].copy()
    d["n"] = d.max_index + 1
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.scatter(d.n, d.index_bit_width, s=70, color=BLUE, alpha=0.55,
               edgecolor="white", linewidth=1.5, zorder=3)
    xs = [2 ** k for k in range(1, 19)]
    ys = [k for k in range(1, 19)]                  # n in (2^(k-1), 2^k] needs k bits
    ax.step(xs, ys, where="pre", color=MUTED, linewidth=2, zorder=2)
    ax.set_xscale("log", base=2)
    ax.set_xlim(2, 2 ** 18 + 2 ** 16)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"2^{int(round(math.log2(v)))}"))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xlabel("largest dictionary index on the page + 1 (log2 scale)")
    ax.set_ylabel("index bit width")
    style(ax)
    title(ax, "Index bit width vs largest dictionary index, per data page")
    ax.legend(handles=[
        Line2D([], [], marker="o", linestyle="", markersize=9, color=BLUE, alpha=0.7,
               label=f"data page ({len(d)} pages, all columns and row groups)"),
        Line2D([], [], color=MUTED, linewidth=2, label="bits needed: ceil(log2(index + 1))")],
        loc="upper left", frameon=False, fontsize=12)
    fig.tight_layout()
    fig.savefig(FIG / "10_bit_width_vs_max_index.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------- 11
def fig_access(access):
    d = access.reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(11, 4.6))
    for y, r in d.iterrows():
        ax.plot([r.csv_bytes, r.parquet_bytes], [y, y], color=GRID, linewidth=2, zorder=1)
        for x, colour in ((r.csv_bytes, BLUE), (r.parquet_bytes, RED)):
            ax.scatter(x, y, s=110, color=colour, edgecolor="white", linewidth=2, zorder=3)
            ax.annotate(human_bytes(x), (x, y), xytext=(0, 12), textcoords="offset points",
                        ha="center", fontsize=11, color=INK)
    ax.set_xscale("log")
    ax.set_xlim(30, 1e9)
    ax.xaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.set_yticks(d.index, d.access)
    ax.set_ylim(len(d) - 0.4, -0.7)
    ax.set_xlabel("bytes read (log scale)")
    style(ax)
    title(ax, "Bytes read: CSV vs Parquet, by access pattern (1,000,000 rows)")
    ax.legend(handles=[
        Line2D([], [], marker="o", linestyle="", markersize=10, color=BLUE, label="CSV"),
        Line2D([], [], marker="o", linestyle="", markersize=10, color=RED, label="Parquet (ZSTD)")],
        loc="upper center", bbox_to_anchor=(0.5, -0.22), ncol=2, frameon=False, fontsize=12)
    fig.tight_layout()
    fig.savefig(FIG / "11_bytes_read_csv_vs_parquet.png", dpi=150)
    plt.close(fig)


def main():
    pages = pd.read_csv("data/results_pages.csv")
    cols = pd.read_csv("data/results_column_bytes.csv")
    access = pd.read_csv("data/results_access.csv")
    fig_page_encoding(pages, list(cols.column))
    fig_column_stages(cols)
    fig_bit_width(pages)
    fig_access(access)
    print(f"wrote figures 08-11 to {FIG}/")


if __name__ == "__main__":
    main()