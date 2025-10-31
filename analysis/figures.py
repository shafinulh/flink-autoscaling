#!/usr/bin/env python3
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DATA_COLUMNS = [
    "Query",
    "Block Cache (MiB)",
    "Parallelism",
    "Total Job Memory (MiB)",
    "Events",
    "Cores",
    "Time (s)",
    "Throughput (K/s)",
]


def load_data(csv_path):
    df = pd.read_csv(csv_path)

    missing = [c for c in DATA_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV missing columns: {', '.join(missing)}")

    numeric = [
        "Block Cache (MiB)",
        "Parallelism",
        "Total Job Memory (MiB)",
        "Events",
        "Cores",
        "Time (s)",
        "Throughput (K/s)",
    ]
    for name in numeric:
        df[name] = pd.to_numeric(df[name], errors="coerce")

    if df["Parallelism"].isna().any():
        raise RuntimeError("Parallelism column had non-numeric junk.")

    df["Parallelism"] = df["Parallelism"].astype(int)
    df = df.sort_values(["Query", "Parallelism", "Block Cache (MiB)"]).reset_index(drop=True)

    if "Total Job Memory (MiB)" not in df or df["Total Job Memory (MiB)"].isna().all():
        df["Total Job Memory (MiB)"] = df["Block Cache (MiB)"] * df["Parallelism"]

    return df


def color_map(values):
    cmap = plt.get_cmap("Set2")
    colors = {}
    for idx, value in enumerate(sorted(values)):
        colors[value] = cmap(idx % cmap.N)
    return colors


def annotate_speedup(ax, x, y, baseline):
    if np.isnan(y) or baseline is None or np.isnan(baseline) or baseline <= 0:
        return
    if y <= 0:
        return
    ratio = y / baseline
    if ratio <= 0:
        return
    ax.text(x, y * 1.02 + 0.5, f"{ratio:.2f}×", ha="center", va="bottom", fontsize=8)


def plot_grouped_bar(df, category, category_label, title, output_path, colors, dpi):
    parallelisms = sorted(df["Parallelism"].unique())
    categories = sorted(df[category].unique())
    x_positions = np.arange(len(categories), dtype=float)
    width = 0.8 / max(len(parallelisms), 1)

    fig, ax = plt.subplots(figsize=(10, 6))
    baseline = (
        df[df["Parallelism"] == 1]
        .set_index(category)["Throughput (K/s)"]
        if 1 in parallelisms
        else pd.Series(dtype=float)
    )

    for idx, parallelism in enumerate(parallelisms):
        subset = df[df["Parallelism"] == parallelism].set_index(category)["Throughput (K/s)"]
        heights = [subset.get(cat, np.nan) for cat in categories]
        offsets = x_positions + idx * width
        ax.bar(
            offsets,
            heights,
            width=width,
            label=f"P={parallelism}",
            color=colors.get(parallelism),
        )
        if parallelism != 1 and not baseline.empty:
            for x_val, y_val, cat in zip(offsets, heights, categories):
                annotate_speedup(ax, x_val, y_val, baseline.get(cat, np.nan))

    tick_positions = x_positions + (len(parallelisms) - 1) * width / 2
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([str(int(cat)) for cat in categories])
    ax.set_xlabel(category_label)
    ax.set_ylabel("Throughput (K/s)")
    ax.set_title(title)
    ax.legend(loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.6)
    ax.set_ylim(bottom=0)
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=dpi)
    plt.close(fig)
    return output_path


def plot_line_by_cache(df, output_path, colors, dpi):
    clean = df.dropna(subset=["Throughput (K/s)", "Block Cache (MiB)"]).copy()
    clean.sort_values(["Parallelism", "Block Cache (MiB)"], inplace=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    for parallelism in sorted(clean["Parallelism"].unique()):
        subset = clean[clean["Parallelism"] == parallelism]
        ax.plot(
            subset["Block Cache (MiB)"],
            subset["Throughput (K/s)"],
            marker="o",
            linewidth=2,
            label=f"P={parallelism}",
            color=colors.get(parallelism),
        )

    ax.set_title("Throughput vs Block Cache Size")
    ax.set_xlabel("Block Cache Size (MiB per instance)")
    ax.set_ylabel("Throughput (K/s)")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.6)
    unique_cache_sizes = clean["Block Cache (MiB)"].unique()
    if len(unique_cache_sizes) > 1 and np.all(unique_cache_sizes > 0):
        ax.set_xscale("log", base=2)
    ax.set_xticks(unique_cache_sizes)
    ax.set_xticklabels([str(int(cs)) for cs in unique_cache_sizes])
    ax.set_ylim(bottom=0)
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=dpi)
    plt.close(fig)
    return output_path


def generate_figures_for_query(df, query, output_root, dpi):
    query_dir = output_root / query
    query_dir.mkdir(parents=True, exist_ok=True)

    parallelisms = sorted(df["Parallelism"].unique())
    colors = color_map(parallelisms)

    figures = []
    figures.append(
        plot_grouped_bar(
            df,
            category="Block Cache (MiB)",
            category_label="Block Cache Size (MiB per instance)",
            title=f"{query}: Throughput by Cache Size and Parallelism",
            output_path=query_dir / "throughput_by_cache.png",
            colors=colors,
            dpi=dpi,
        )
    )
    figures.append(
        plot_grouped_bar(
            df,
            category="Total Job Memory (MiB)",
            category_label="Total Job Memory (MiB)",
            title=f"{query}: Throughput by Total Job Memory and Parallelism",
            output_path=query_dir / "throughput_by_total_memory.png",
            colors=colors,
            dpi=dpi,
        )
    )
    figures.append(
        plot_line_by_cache(
            df,
            output_path=query_dir / "throughput_vs_cache.png",
            colors=colors,
            dpi=dpi,
        )
    )
    return figures


def main():
    root = Path(__file__).resolve()
    default_csv = root.with_name("init_experiments.csv")
    if len(sys.argv) > 2:
        raise SystemExit("usage: figures.py [csv_path]")
    csv_path = Path(sys.argv[1]) if len(sys.argv) == 2 else default_csv
    df = load_data(csv_path)

    output_dir = root.with_name("figures")
    dpi = 150

    available_queries = sorted(df["Query"].unique())
    if not available_queries:
        print("Nothing to plot.")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)

    summary = []
    for query, query_df in df.groupby("Query"):
        figures = generate_figures_for_query(query_df, query, output_dir, dpi)
        summary.append((query, len(figures)))

    for query, count in sorted(summary):
        print(f"{query}: dumped {count} figure(s) to {output_dir / query}")
    print(f"All figures land in {output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    main()
