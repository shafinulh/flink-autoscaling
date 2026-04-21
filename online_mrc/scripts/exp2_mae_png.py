"""
exp2_mae_png.py — Static MAE convergence figure for Experiment 2.

Plots MAE (mean absolute error vs per-snapshot ground truth) across snapshots
for each sampling rate. Each SHARDS snapshot N is compared against the GT
snapshot at the same point in the access stream (gt_snapshot_N.txt), so the
curve measures SHARDS approximation quality at each moment — not convergence
toward the final answer.

Usage:
  python3 exp2_mae_png.py \\
    --results-dir      benchmark/online-mrc/results \\
    --gt-snapshots-dir benchmark/online-mrc/results/old_experiments/exp2_trace/analysis/gt_snapshots \\
    --shards-runs      exp2_shards_s1 exp2_shards_s01 exp2_shards_s001 exp2_shards_s0001 \\
    --labels           "s=1.0" "s=0.1" "s=0.01" "s=0.001" \\
    --sampling-ratios  1.0 0.1 0.01 0.001 \\
    --interval         1000000 \\
    --output           benchmark/online-mrc/results/exp2_analysis/exp2_mae.png
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


AVG_BS = 4080.0
COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']


# ---------------------------------------------------------------------------
# Readers
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


def read_gt_snapshot(path: Path):
    """Parse gt_snapshot_N.txt produced by gen_gt_snapshots.sh."""
    caps, mrs = [], []
    in_mrc = False
    with open(path) as f:
        for line in f:
            line = line.strip()
            if 'miss ratio curve' in line:
                in_mrc = True
                continue
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


def interpolate(xs, ys, target):
    valid = [(x, y) for x, y in zip(xs, ys) if x > 0 and 0.0 <= y <= 1.0]
    if not valid:
        return None
    vxs, vys = zip(*valid)
    if target <= vxs[0]:  return vys[0]
    if target >= vxs[-1]: return vys[-1]
    for i in range(1, len(vxs)):
        if vxs[i] >= target:
            t = (target - vxs[i-1]) / (vxs[i] - vxs[i-1])
            return vys[i-1] + t * (vys[i] - vys[i-1])
    return None


def collect_snapshots(run_dir: Path, sampling_ratio: float):
    snaps = []
    for p in run_dir.iterdir():
        name = p.name
        if name.startswith('online_mrc.bin.') and name.endswith('.txt'):
            stem = name[len('online_mrc.bin.'):-len('.txt')]
            try:
                n = int(stem)
            except ValueError:
                continue
            xs, ys = read_shards_txt(p, sampling_ratio)
            snaps.append((n, xs, ys))
    snaps.sort(key=lambda s: s[0])
    return snaps


def compute_mae(shards_xs, shards_ys, gt_xs, gt_ys):
    errors = []
    for gx, gy in zip(gt_xs, gt_ys):
        pred = interpolate(shards_xs, shards_ys, gx)
        if pred is not None:
            errors.append(abs(pred - gy))
    if not errors:
        return None
    return sum(errors) / len(errors) * 100.0   # percent


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

def make_plot(results_dir, gt_snapshots_dir, shards_runs, labels,
              sampling_ratios, interval, output):

    fig, ax = plt.subplots(figsize=(6, 4))

    for run_name, label, s, color in zip(shards_runs, labels, sampling_ratios, COLORS):
        run_dir = results_dir / run_name
        snaps = collect_snapshots(run_dir, s)
        if not snaps:
            print(f"  WARNING: no snapshots in {run_dir}")
            continue

        snap_nums = [n for n, _, _ in snaps]
        maes = []
        for n, xs, ys in snaps:
            gt_path = gt_snapshots_dir / f'gt_snapshot_{n}.txt'
            if not gt_path.exists():
                print(f"  WARNING: GT snapshot not found: {gt_path}")
                maes.append(0.0)
                continue
            gt_xs, gt_ys = read_gt_snapshot(gt_path)
            mae = compute_mae(xs, ys, gt_xs, gt_ys)
            maes.append(mae if mae is not None else 0.0)

        x_axis = [n * interval / 1_000_000 for n in snap_nums]  # millions of accesses
        ax.plot(x_axis, maes, color=color, linewidth=1.5, marker='o',
                markersize=3, label=label)

    ax.set_xlabel('Block accesses seen (millions)', fontsize=10)
    ax.set_ylabel('MAE vs ground truth (%)', fontsize=10)
    ax.set_ylim(bottom=0)
    ax.grid(True, linestyle=':', linewidth=0.5, color='#cccccc')
    ax.legend(fontsize=8, loc='upper right', framealpha=0.9)
    ax.tick_params(labelsize=8)

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
    ap.add_argument('--gt-snapshots-dir', type=Path, required=True,
                    help='Directory containing gt_snapshot_N.txt files')
    ap.add_argument('--shards-runs',     type=str,  nargs='+',
                    default=['exp2_shards_s1','exp2_shards_s01',
                             'exp2_shards_s001','exp2_shards_s0001'])
    ap.add_argument('--labels',          type=str,  nargs='+',
                    default=['s=1.0','s=0.1','s=0.01','s=0.001'])
    ap.add_argument('--sampling-ratios', type=float, nargs='+',
                    default=[1.0, 0.1, 0.01, 0.001])
    ap.add_argument('--interval',        type=int,  default=1_000_000)
    ap.add_argument('--output',          type=Path,
                    default=Path('benchmark/online-mrc/results/exp2_analysis/exp2_mae.png'))
    args = ap.parse_args()

    make_plot(args.results_dir, args.gt_snapshots_dir,
              args.shards_runs, args.labels, args.sampling_ratios,
              args.interval, args.output)


if __name__ == '__main__':
    main()
