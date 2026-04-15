"""
Compare SHARDS MRC against the RocksDB ground-truth MRC.
Pure stdlib — no numpy or matplotlib required.

Outputs:
  - A text table to stdout
  - An SVG plot to --output (viewable in any browser)

SHARDS bin format:
  header : (num_bins: u64, bin_size: u64)
  entries: (index: u64, miss-rate: f64) * N

Ground-truth format (mrc.txt sections):
  Lines inside "===== miss ratio curve =====" section:
    cache_name,num_shard_bits,ghost_capacity,capacity,miss_ratio,total_accesses
  - capacity  = bytes
  - miss_ratio = 0..100 (percent)

Usage:
  python3 scripts/compare_shards_vs_groundtruth.py \\
    --shards   results/exp1_shards/online_mrc.bin \\
    --truth    results/exp1_trace/analysis/mrc.txt \\
    --trace-csv results/exp1_trace/analysis/human.csv \\
    --output   results/exp1_shards/exp1_comparison.svg \\
    --title    "Experiment 1: Online SHARDS (s=1) vs Ground Truth MRC"
"""

import argparse
import csv
import math
import struct
from pathlib import Path


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_shards_bin(path: Path):
    """Return (cache_sizes_blocks: list[int], miss_rates: list[float])."""
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) < 16:
        raise ValueError(f"SHARDS binary too short: {len(raw)} bytes")
    num_bins, bin_size = struct.unpack_from("<QQ", raw, 0)
    offset = 16
    entry_size = 8 + 8  # u64 + f64
    indices = []
    miss_rates = []
    while offset + entry_size <= len(raw):
        idx, mr = struct.unpack_from("<Qd", raw, offset)
        indices.append(idx)
        miss_rates.append(mr)
        offset += entry_size
    return indices, miss_rates, num_bins, bin_size


def avg_block_size_from_csv(trace_csv: Path) -> float:
    """Compute average data block size (block_type=9) from human-readable trace CSV."""
    BLOCK_TYPE_DATA = 9
    total_size = 0
    count = 0
    max_entries = 5_000_000
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
        raise ValueError("No nonzero data block entries found in trace CSV")
    avg = total_size / count
    print(f"  Avg data block size: {avg:.1f} B  ({count:,} entries sampled)")
    return avg


def read_ground_truth(path: Path):
    """Return (capacities_bytes: list[float], miss_ratios_01: list[float])."""
    capacities = []
    miss_ratios = []
    in_mrc_section = False
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if "miss ratio curve" in line:
                in_mrc_section = True
                continue
            if line.startswith("=====") and in_mrc_section:
                break
            if not in_mrc_section or not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) < 5:
                continue
            try:
                capacity   = float(parts[3])
                miss_ratio = float(parts[4])
            except ValueError:
                continue
            capacities.append(capacity)
            miss_ratios.append(miss_ratio / 100.0)
    return capacities, miss_ratios


# ---------------------------------------------------------------------------
# Text table
# ---------------------------------------------------------------------------

def fmt_bytes(b: float) -> str:
    for unit, threshold in [("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)]:
        if b >= threshold:
            return f"{b / threshold:.1f} {unit}"
    return f"{b:.0f} B"


def print_table(truth_bytes, truth_mr, shards_bytes, shards_mr):
    print("\n=== Ground Truth MRC ===")
    print(f"{'Cache Size':>12}  {'Miss Ratio':>10}")
    print("-" * 26)
    for cap, mr in zip(truth_bytes, truth_mr):
        print(f"{fmt_bytes(cap):>12}  {mr:>10.4f}")

    print("\n=== SHARDS MRC (sampled bins with miss_rate <= 1.0) ===")
    print(f"{'Cache Size':>12}  {'Miss Rate':>10}")
    print("-" * 26)
    shown = 0
    for cap, mr in zip(shards_bytes, shards_mr):
        if mr > 1.0:
            continue
        print(f"{fmt_bytes(cap):>12}  {mr:>10.4f}")
        shown += 1
        if shown >= 40:
            print("  ... (truncated)")
            break


def interpolate_shards_at(shards_bytes, shards_mr, target_bytes):
    """Linear interpolation of shards MRC at a given target cache size (bytes)."""
    # filter valid
    valid = [(b, m) for b, m in zip(shards_bytes, shards_mr) if 0.0 <= m <= 1.0]
    if not valid:
        return None
    xs, ys = zip(*valid)
    if target_bytes <= xs[0]:
        return ys[0]
    if target_bytes >= xs[-1]:
        return ys[-1]
    for i in range(1, len(xs)):
        if xs[i] >= target_bytes:
            t = (target_bytes - xs[i-1]) / (xs[i] - xs[i-1])
            return ys[i-1] + t * (ys[i] - ys[i-1])
    return None


def print_comparison_table(truth_bytes, truth_mr, shards_bytes, shards_mr):
    print("\n=== Comparison at Ground-Truth Cache Sizes ===")
    print(f"{'Cache Size':>12}  {'Truth MR':>10}  {'SHARDS MR':>10}  {'Delta':>8}")
    print("-" * 48)
    for cap, tmr in zip(truth_bytes, truth_mr):
        smr = interpolate_shards_at(shards_bytes, shards_mr, cap)
        if smr is None:
            delta_str = "N/A"
            smr_str   = "N/A"
        else:
            delta = smr - tmr
            delta_str = f"{delta:+.4f}"
            smr_str   = f"{smr:.4f}"
        print(f"{fmt_bytes(cap):>12}  {tmr:>10.4f}  {smr_str:>10}  {delta_str:>8}")


# ---------------------------------------------------------------------------
# SVG plot (no external deps)
# ---------------------------------------------------------------------------

def to_svg(title, truth_bytes, truth_mr, shards_bytes, shards_mr,
           out_path: Path, avg_bs: float):
    W, H = 960, 600
    PAD_L, PAD_R, PAD_T, PAD_B = 90, 40, 50, 60

    plot_w = W - PAD_L - PAD_R
    plot_h = H - PAD_T - PAD_B

    # --- x axis: log scale over all data ---
    all_x = [b for b in shards_bytes if b > 0] + [b for b in truth_bytes if b > 0]
    x_min = min(all_x)
    x_max = max(all_x)
    log_min = math.log10(x_min)
    log_max = math.log10(x_max)

    def px(b):
        if b <= 0:
            return PAD_L
        return PAD_L + (math.log10(b) - log_min) / (log_max - log_min) * plot_w

    def py(mr):
        mr = max(0.0, min(1.0, mr))
        return PAD_T + (1.0 - mr) * plot_h

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
                 f'font-family="monospace" font-size="12">')

    # background
    lines.append(f'<rect width="{W}" height="{H}" fill="white"/>')

    # title
    lines.append(f'<text x="{W//2}" y="30" text-anchor="middle" '
                 f'font-size="15" font-weight="bold">{title}</text>')

    # plot border
    lines.append(f'<rect x="{PAD_L}" y="{PAD_T}" width="{plot_w}" height="{plot_h}" '
                 f'fill="none" stroke="#aaa" stroke-width="1"/>')

    # grid lines (powers of 10)
    lo_p = math.ceil(log_min)
    hi_p = math.floor(log_max)
    for p in range(int(lo_p), int(hi_p) + 1):
        xg = px(10**p)
        lines.append(f'<line x1="{xg:.1f}" y1="{PAD_T}" x2="{xg:.1f}" '
                     f'y2="{PAD_T+plot_h}" stroke="#ddd" stroke-dasharray="4 4"/>')
        label = f"10^{p}" if p < 4 else f"{10**p // 1000}K" if p < 7 else f"{10**p // 1000000}M"
        # nicer labels
        val = 10**p
        if val < 1024:
            label = f"{val}B"
        elif val < 1024**2:
            label = f"{val//1024}KB"
        elif val < 1024**3:
            label = f"{val//1024**2}MB"
        else:
            label = f"{val//1024**3}GB"
        lines.append(f'<text x="{xg:.1f}" y="{PAD_T+plot_h+18}" text-anchor="middle" '
                     f'fill="#666">{label}</text>')

    for mr_tick in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
        yg = py(mr_tick)
        lines.append(f'<line x1="{PAD_L}" y1="{yg:.1f}" x2="{PAD_L+plot_w}" '
                     f'y2="{yg:.1f}" stroke="#ddd" stroke-dasharray="4 4"/>')
        lines.append(f'<text x="{PAD_L-6}" y="{yg+4:.1f}" text-anchor="end" '
                     f'fill="#666">{mr_tick:.1f}</text>')

    # axis labels
    lines.append(f'<text x="{PAD_L + plot_w//2}" y="{H-8}" text-anchor="middle" '
                 f'fill="#333">Cache capacity (bytes, log scale)</text>')
    cx = 14
    cy = PAD_T + plot_h // 2
    lines.append(f'<text x="{cx}" y="{cy}" text-anchor="middle" fill="#333" '
                 f'transform="rotate(-90,{cx},{cy})">Miss rate</text>')

    # --- SHARDS line ---
    valid_s = [(b, m) for b, m in zip(shards_bytes, shards_mr) if 0.0 <= m <= 1.0 and b > 0]
    if len(valid_s) >= 2:
        pts = " ".join(f"{px(b):.1f},{py(m):.1f}" for b, m in valid_s)
        lines.append(f'<polyline points="{pts}" fill="none" stroke="steelblue" stroke-width="2"/>')

    # --- Ground truth line + dots ---
    valid_t = [(b, m) for b, m in zip(truth_bytes, truth_mr) if b > 0]
    if len(valid_t) >= 2:
        pts = " ".join(f"{px(b):.1f},{py(m):.1f}" for b, m in valid_t)
        lines.append(f'<polyline points="{pts}" fill="none" stroke="orange" stroke-width="2"/>')
    for b, m in valid_t:
        lines.append(f'<circle cx="{px(b):.1f}" cy="{py(m):.1f}" r="4" '
                     f'fill="orange" stroke="white" stroke-width="1"/>')

    # --- Legend ---
    lx, ly = PAD_L + 20, PAD_T + 20
    lines.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+30}" y2="{ly}" '
                 f'stroke="steelblue" stroke-width="2"/>')
    lines.append(f'<text x="{lx+36}" y="{ly+4}" fill="#333">'
                 f'SHARDS (avg block {avg_bs:.0f} B)</text>')
    ly2 = ly + 22
    lines.append(f'<line x1="{lx}" y1="{ly2}" x2="{lx+30}" y2="{ly2}" '
                 f'stroke="orange" stroke-width="2"/>')
    lines.append(f'<circle cx="{lx+15}" cy="{ly2}" r="4" fill="orange"/>')
    lines.append(f'<text x="{lx+36}" y="{ly2+4}" fill="#333">Ground truth (LRU sim)</text>')

    lines.append('</svg>')

    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(f"SVG plot saved to: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards",     type=Path, required=True)
    parser.add_argument("--truth",      type=Path, required=True)
    parser.add_argument("--trace-csv",  type=Path, required=True)
    parser.add_argument("--output",     type=Path, default=Path("comparison.svg"))
    parser.add_argument("--title",      type=str,  default="SHARDS vs Ground-Truth MRC")
    args = parser.parse_args()

    print("Reading SHARDS binary MRC...")
    s_indices, s_mr, num_bins, bin_size = read_shards_bin(args.shards)
    print(f"  {len(s_indices)} bins, num_bins={num_bins}, bin_size={bin_size}")

    print("Computing average block size from trace CSV...")
    avg_bs = avg_block_size_from_csv(args.trace_csv)

    s_bytes = [idx * bin_size * avg_bs for idx in s_indices]

    print("Reading ground-truth MRC...")
    t_bytes, t_mr = read_ground_truth(args.truth)
    print(f"  {len(t_bytes)} ground-truth points")

    print_table(t_bytes, t_mr, s_bytes, s_mr)
    print_comparison_table(t_bytes, t_mr, s_bytes, s_mr)

    to_svg(args.title, t_bytes, t_mr, s_bytes, s_mr, args.output, avg_bs)


if __name__ == "__main__":
    main()
