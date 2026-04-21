"""
exp2_mrc_overlay.py — Static MRC overlay figure for Experiment 2.

Plots the final-snapshot SHARDS MRC for all sampling rates overlaid on the
ground truth LRU simulation, in the style of SHARDS paper Figure 3(a).
Outputs a publication-quality PNG.

Usage:
  python3 exp2_mrc_overlay.py \\
    --results-dir  benchmark/online-mrc/results \\
    --truth-mrc    benchmark/online-mrc/results/old_experiments/exp2_trace/analysis/mrc.txt \\
    --shards-runs  exp2_shards_s1 exp2_shards_s01 exp2_shards_s001 exp2_shards_s0001 exp2_shards_s0001_extra exp2_shards_s00001_extra \\
    --labels       "s=1.0" "s=0.1" "s=0.01" "s=0.001" "s=0.0001" "s=0.00001" \\
    --sampling-ratios 1.0 0.1 0.01 0.001 0.0001 0.00001 \\
    --output       benchmark/online-mrc/results/exp2_analysis/exp2_mrc_overlay.png
"""

import argparse
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


AVG_BS = 4080.0
X_MIN  = 1   * 1024 ** 2   # 1 MB
X_MAX  = 16  * 1024 ** 3   # 16 GiB

COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']


# ---------------------------------------------------------------------------
# Readers (same logic as exp2_animation.py)
# ---------------------------------------------------------------------------

def read_shards_txt(path: Path, sampling_ratio: float):
    xs, ys = [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('cache'):
                continue
            parts = line.split(',')
            if len(parts) < 2:
                continue
            try:
                cache_units = float(parts[0])
                miss_ratio  = float(parts[1])
            except ValueError:
                continue
            if cache_units <= 0:
                continue
            xs.append(cache_units * AVG_BS / sampling_ratio)
            ys.append(miss_ratio)
    return xs, ys


def read_ground_truth(path: Path):
    caps, mrs = [], []
    in_mrc = False
    with open(path) as f:
        for line in f:
            line = line.strip()
            if 'miss ratio curve' in line:
                in_mrc = True
                continue
            if line.startswith('=====') and in_mrc:
                break
            if not in_mrc or not line or line.startswith('#'):
                continue
            parts = line.split(',')
            if len(parts) < 5:
                continue
            try:
                cap = float(parts[3])
                mr  = float(parts[4]) / 100.0
            except ValueError:
                continue
            caps.append(cap)
            mrs.append(mr)
    return caps, mrs


def get_final_snapshot(run_dir: Path, sampling_ratio: float):
    """Return (xs, ys) for the highest-numbered snapshot .txt in run_dir."""
    snaps = []
    for p in run_dir.iterdir():
        name = p.name
        if name.startswith('online_mrc.bin.') and name.endswith('.txt'):
            stem = name[len('online_mrc.bin.'):-len('.txt')]
            try:
                snaps.append((int(stem), p))
            except ValueError:
                continue
    if not snaps:
        return [], []
    _, path = max(snaps, key=lambda x: x[0])
    return read_shards_txt(path, sampling_ratio)


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

def make_plot(results_dir, truth_mrc_path, shards_runs, labels, sampling_ratios, output):
    fig, ax = plt.subplots(figsize=(6, 4))

    # Ground truth
    gt_xs, gt_ys = read_ground_truth(truth_mrc_path)
    gt_xs_f = [x for x in gt_xs if X_MIN <= x <= X_MAX]
    gt_ys_f = [y for x, y in zip(gt_xs, gt_ys) if X_MIN <= x <= X_MAX]
    ax.plot(gt_xs_f, gt_ys_f, 'ko', markersize=4, label='Ground truth', zorder=10)

    # SHARDS curves
    for run_name, label, s, color in zip(shards_runs, labels, sampling_ratios, COLORS):
        run_dir = results_dir / run_name
        xs, ys = get_final_snapshot(run_dir, s)
        if not xs:
            print(f"  WARNING: no snapshots in {run_dir}")
            continue
        xs_f = [x for x in xs if X_MIN <= x <= X_MAX]
        ys_f = [y for x, y in zip(xs, ys) if X_MIN <= x <= X_MAX]
        ax.plot(xs_f, ys_f, color=color, linewidth=1.0, linestyle='--', label=label)

    ax.set_xscale('log')
    ax.set_xlim(X_MIN, X_MAX)
    ax.set_ylim(-0.02, 1.02)

    # x-axis ticks at powers of 2, labels every other
    p2_start = math.ceil(math.log2(X_MIN))
    p2_end   = math.floor(math.log2(X_MAX))
    ticks = [2**p for p in range(p2_start, p2_end + 1)]
    ax.set_xticks(ticks)

    def fmt_bytes(b):
        if b >= 1 << 30: return f'{b >> 30}GB'
        if b >= 1 << 20: return f'{b >> 20}MB'
        if b >= 1 << 10: return f'{b >> 10}KB'
        return str(b)

    ax.set_xticklabels([fmt_bytes(t) if math.log2(t) % 2 == 0 else ''
                        for t in ticks], fontsize=8)
    ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['0.0', '0.2', '0.4', '0.6', '0.8', '1.0'], fontsize=8)

    ax.set_xlabel('Cache size', fontsize=10)
    ax.set_ylabel('Miss ratio', fontsize=10)
    ax.grid(True, which='both', linestyle=':', linewidth=0.5, color='#cccccc')
    leg = ax.legend(fontsize=8, loc='upper right', framealpha=0.9)
    leg.set_title('Sampling Rate', prop={'size': 8})

    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {output}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--results-dir',      type=Path, required=True)
    ap.add_argument('--truth-mrc',        type=Path, required=True)
    ap.add_argument('--shards-runs',      type=str,  nargs='+',
                    default=['exp2_shards_s1','exp2_shards_s01','exp2_shards_s001',
                             'exp2_shards_s0001','exp2_shards_s0001_extra','exp2_shards_s00001_extra'])
    ap.add_argument('--labels',           type=str,  nargs='+',
                    default=['s=1.0','s=0.1','s=0.01','s=0.001','s=0.0001','s=0.00001'])
    ap.add_argument('--sampling-ratios',  type=float, nargs='+',
                    default=[1.0, 0.1, 0.01, 0.001, 0.0001, 0.00001])
    ap.add_argument('--output',           type=Path,
                    default=Path('benchmark/online-mrc/results/exp2_analysis/exp2_mrc_overlay.png'))
    args = ap.parse_args()

    make_plot(args.results_dir, args.truth_mrc,
              args.shards_runs, args.labels, args.sampling_ratios,
              args.output)


if __name__ == '__main__':
    main()
