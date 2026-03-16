"""
Overlay SHARDS MRC against the RocksDB ground-truth MRC for q20.

SHARDS bin format (from online_mrc):
  header : (num_bins: u64, bin_size: u64)
  entries: (index: u64, miss-rate: f64) * N
  - index  = number of unique blocks
  - miss-rate = 0..1

Ground-truth CSV format (from block_cache_trace_analyzer -cache_sim_config):
  cache_name,num_shard_bits,ghost_capacity,capacity,miss_ratio,total_accesses
  - capacity  = bytes
  - miss_ratio = 0..100 (percent)

To put both on the same x-axis (bytes) we multiply SHARDS's block-count
x-values by the average block size computed from the human-readable trace CSV.

Usage:
  python3 scripts/plot_shards_vs_groundtruth.py \\
    --shards    q20-shards-mrc.bin \\
    --truth     /mnt/home/Tomasdfgh/distr_project/mrc_q20_unique.txt \\
    --trace-csv /tmp/q20_human.txt \\
    --output    q20-shards-vs-truth.png
"""

import argparse
import struct
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt


def read_shards_bin(path: Path):
    """Return (indices_as_blocks: np.ndarray[u64], miss_rates: np.ndarray[f64])."""
    with open(path, "rb") as f:
        header = np.fromfile(f, dtype=np.dtype([("num_bins", np.uint64), ("bin_size", np.uint64)]), count=1)
        entries = np.fromfile(f, dtype=np.dtype([("index", np.uint64), ("miss-rate", np.float64)]))
    return entries["index"], entries["miss-rate"]


def avg_block_size_from_csv(trace_csv: Path) -> float:
    """
    Compute average block size (bytes) from a human-readable RocksDB trace CSV,
    using data blocks only (block_type=9). Data blocks dominate cache accesses
    numerically and are what SHARDS primarily samples; including large index/filter
    blocks inflates the average and shifts the SHARDS x-axis far to the right.
    Column layout: timestamp, block_id, block_type, block_size, ...
    We sample up to 5M nonzero data-block entries to keep it fast.
    """
    total_size = 0
    count = 0
    max_entries = 5_000_000
    BLOCK_TYPE_DATA = 9
    with open(trace_csv, "r") as f:
        for line in f:
            if count >= max_entries:
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) < 4:
                continue
            try:
                block_type = int(parts[2])
                if block_type != BLOCK_TYPE_DATA:
                    continue
                block_size = int(parts[3])
                if block_size <= 0:
                    continue
                total_size += block_size
                count += 1
            except ValueError:
                continue
    if count == 0:
        raise ValueError("Could not read any nonzero data block entries from trace CSV")
    avg = total_size / count
    print(f"  Average data block size: {avg:.1f} bytes  (sampled {count:,} nonzero data block entries)")
    return avg


def read_ground_truth(path: Path):
    """
    Return (capacities_bytes: np.ndarray[f64], miss_ratios_01: np.ndarray[f64])
    from RocksDB cache-sim CSV.
    """
    capacities = []
    miss_ratios = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("=") or line.startswith("cache_name"):
                continue
            parts = line.split(",")
            if len(parts) < 5:
                continue
            try:
                capacity   = float(parts[3])   # bytes
                miss_ratio = float(parts[4])    # 0..100
            except ValueError:
                continue
            capacities.append(capacity)
            miss_ratios.append(miss_ratio / 100.0)
    return np.array(capacities), np.array(miss_ratios)


def main():
    parser = argparse.ArgumentParser(
        description="Plot SHARDS MRC vs RocksDB ground-truth MRC for q20"
    )
    parser.add_argument("--shards",     type=Path, required=True, help="SHARDS .bin output")
    parser.add_argument("--truth",      type=Path, required=True, help="Ground-truth CSV from block_cache_trace_analyzer")
    parser.add_argument("--trace-csv",  type=Path, required=True, help="Human-readable trace CSV (to compute avg block size)")
    parser.add_argument("--output",     type=Path, default=Path("q20-shards-vs-truth.png"), help="Output plot path")
    parser.add_argument("--title",      type=str, default="q20: SHARDS vs Ground-Truth MRC", help="Plot title")
    args = parser.parse_args()

    print("Reading SHARDS MRC...")
    shards_blocks, shards_mr = read_shards_bin(args.shards)

    print("Computing average block size from trace CSV...")
    avg_bs = avg_block_size_from_csv(args.trace_csv)

    shards_bytes = shards_blocks.astype(np.float64) * avg_bs

    print("Reading ground-truth MRC...")
    truth_bytes, truth_mr = read_ground_truth(args.truth)

    print("Plotting...")
    fig, ax = plt.subplots(figsize=(12, 8), dpi=150)

    ax.step(shards_bytes, shards_mr, where="post", label=f"SHARDS (avg block size {avg_bs:.0f} B)", color="steelblue")
    ax.plot(truth_bytes, truth_mr, marker="o", markersize=4, label="Ground truth (RocksDB cache sim)", color="orange")

    ax.set_xscale("log")
    ax.set_xlabel("Cache capacity (bytes, log scale)")
    ax.set_ylabel("Miss rate")
    ax.set_ylim(0, 1.05)
    ax.set_title(args.title)
    ax.legend()
    ax.grid(True, which="both", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(args.output)
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
