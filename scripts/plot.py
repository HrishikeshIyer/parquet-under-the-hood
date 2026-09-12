#!/usr/bin/env python3

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

FIG = Path("figures")
FIG.mkdir(exist_ok=True)
sns.set_theme(style="whitegrid", context="talk", palette="deep")


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def label_bars(ax, fmt=human_bytes, rotate=False):
    for p in ax.patches:
        h = p.get_height()
        if h and h == h:  # skip 0/NaN
            ax.annotate(fmt(h), (p.get_x() + p.get_width() / 2, h),
                        ha="center", va="bottom", fontsize=10,
                        rotation=90 if rotate else 0,
                        xytext=(0, 3), textcoords="offset points")


# 1. line — parquet vs csv across the row sweep
def fig_size_vs_rows(df):
    long = df.melt(id_vars="rows", value_vars=["parquet_snappy", "csv"],
                   var_name="format", value_name="bytes")
    plt.figure(figsize=(8, 5))
    ax = sns.lineplot(data=long, x="rows", y="bytes", hue="format",
                      marker="o", linewidth=2.5, markersize=9)
    ax.set(xscale="log", yscale="log", xlabel="rows", ylabel="file size (bytes)")
    ax.set_title("Parquet vs CSV file size across row counts")
    plt.tight_layout()
    plt.savefig(FIG / "01_parquet_vs_csv.png", dpi=150)
    plt.close()


# 2. bar — footer share across the row sweep
def fig_footer_share(df):
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=df, x="rows", y="footer_pct", color=sns.color_palette("deep")[3])
    ax.set(xlabel="rows", ylabel="footer / file size (%)")
    ax.set_title("Footer share of file size across row counts")
    label_bars(ax, fmt=lambda v: f"{v:.2f}%")
    plt.tight_layout()
    plt.savefig(FIG / "02_footer_share_vs_rows.png", dpi=150)
    plt.close()


# 3. bar — file size across codecs (sorted)
def fig_codec_size(df):
    d = df.sort_values("file_bytes")
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=d, x="codec", y="file_bytes", hue="codec",
                     palette="viridis", legend=False)
    ax.set(xlabel="compression codec", ylabel="file size (bytes)")
    ax.set_title("File size across compression codecs")
    label_bars(ax)
    plt.tight_layout()
    plt.savefig(FIG / "03_size_across_codecs.png", dpi=150)
    plt.close()


# 4. scatter — the size/speed Pareto frontier across codecs
def fig_codec_pareto(df):
    plt.figure(figsize=(8, 5.5))
    ax = sns.scatterplot(data=df, x="write_ms", y="file_bytes", hue="codec",
                         s=220, style="codec")
    ax.set(xscale="log", xlabel="write time (ms, log)", ylabel="file size (bytes)")
    ax.set_title("File size vs write time across compression codecs")
    for _, r in df.iterrows():
        ax.annotate(r["codec"], (r["write_ms"], r["file_bytes"]),
                    xytext=(6, 4), textcoords="offset points", fontsize=10)
    ax.legend_.remove()
    plt.tight_layout()
    plt.savefig(FIG / "04_codec_pareto.png", dpi=150)
    plt.close()


# 5. horizontal bar — where the bytes live, by column
def fig_column_sizes(df):
    d = df.sort_values("dict_on_bytes", ascending=True)
    plt.figure(figsize=(8, 6))
    ax = sns.barplot(data=d, y="column", x="dict_on_bytes", hue="column",
                     palette="mako", legend=False)
    ax.set(xlabel="compressed size (bytes)", ylabel="")
    ax.set_title("Compressed size across columns")
    plt.tight_layout()
    plt.savefig(FIG / "05_size_across_columns.png", dpi=150)
    plt.close()


# 6. grouped bar — dictionary encoding on vs off, per column
def fig_dict_effect(df):
    long = df.melt(id_vars="column", value_vars=["dict_on_bytes", "dict_off_bytes"],
                   var_name="dictionary", value_name="bytes")
    long["dictionary"] = long["dictionary"].map(
        {"dict_on_bytes": "on", "dict_off_bytes": "off"})
    order = df.sort_values("dict_off_bytes", ascending=False)["column"]
    plt.figure(figsize=(11, 5.5))
    ax = sns.barplot(data=long, x="column", y="bytes", hue="dictionary", order=order)
    ax.set(xlabel="", ylabel="compressed size (bytes)")
    ax.set_title("Column size with vs without dictionary encoding")
    ax.tick_params(axis="x", rotation=45)
    for lbl in ax.get_xticklabels():
        lbl.set_ha("right")
    plt.tight_layout()
    plt.savefig(FIG / "06_dictionary_on_vs_off.png", dpi=150)
    plt.close()


# 7. two-panel bar — footer and file size across row-group counts
def fig_rowgroups(df):
    d = df.sort_values("num_row_groups")
    d["rg"] = d["num_row_groups"].astype(str)
    fig, (a, b) = plt.subplots(1, 2, figsize=(12, 5))
    sns.barplot(data=d, x="rg", y="footer_bytes", ax=a, color=sns.color_palette("deep")[2])
    a.set(xlabel="row groups", ylabel="footer size (bytes)", yscale="log")
    a.set_title("Footer size vs row-group count")
    sns.barplot(data=d, x="rg", y="file_bytes", ax=b, color=sns.color_palette("deep")[0])
    b.set(xlabel="row groups", ylabel="file size (bytes)")
    b.set_title("File size vs row-group count")
    fig.suptitle("Footer and file size across row-group counts (N = 1,000,000)")
    plt.tight_layout()
    plt.savefig(FIG / "07_rowgroup_tradeoff.png", dpi=150)
    plt.close()


def main():
    fig_size_vs_rows(pd.read_csv("data/results.csv"))
    fig_footer_share(pd.read_csv("data/results.csv"))
    fig_codec_size(pd.read_csv("data/results_compression.csv"))
    fig_codec_pareto(pd.read_csv("data/results_compression.csv"))
    fig_column_sizes(pd.read_csv("data/results_columns.csv"))
    fig_dict_effect(pd.read_csv("data/results_columns.csv"))
    if Path("data/results_rowgroups.csv").exists():
        fig_rowgroups(pd.read_csv("data/results_rowgroups.csv"))
    print(f"wrote figures to {FIG}/")


if __name__ == "__main__":
    main()