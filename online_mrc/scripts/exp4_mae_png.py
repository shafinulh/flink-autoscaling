"""
exp4_mae_png.py — MAE-over-time curves for no-decay vs decay (Exp4).

For each periodic snapshot, compute MAE between the SHARDS MRC and the
target-distribution ground truth, then plot MAE vs snapshot index.

Two panels:
  Left:  forward switch (uniform→Zipfian), GT = Zipfian
  Right: reverse switch (Zipfian→uniform), GT = uniform

Usage:
  python3 exp4_mae_png.py \\
    --forward-no-decay  results/exp3_forward_switch/ \\
    --forward-decay     results/exp4_forward_decay/ \\
    --reverse-no-decay  results/exp3_reverse_switch/ \\
    --reverse-decay     results/exp4_reverse_decay/ \\
    --zipfian-gt        results/old_experiments/.../exp2_trace/analysis/mrc.txt \\
    --uniform-gt        results/exp3_uniform_trace/analysis/mrc.txt \\
    --output            final_report/exp4_mae.png
"""

import argparse
import math
import re
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np

AVG_BS   = 4080.0
SAMPLING = 0.01
X_MIN    = 1  * 1024 ** 2
X_MAX    = 16 * 1024 ** 3
N_INTERP = 500   # log-spaced points for MAE integration

COLOR_NO_DECAY = '#d62728'
COLOR_DECAY    = '#2ca02c'


def read_shards(path: Path):
    xs, ys = [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('cache'):
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
            xs.append(cache_units * AVG_BS / SAMPLING)
            ys.append(miss_ratio)
    return np.array(xs), np.array(ys)


def read_gt(path: Path):
    xs, ys = [], []
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
            if len(parts) < 5 or parts[0] != 'lru':
                continue
            try:
                xs.append(float(parts[3]))
                ys.append(float(parts[4]) / 100.0)
            except ValueError:
                continue
    return np.array(xs), np.array(ys)


def interp_mrc(xs, ys, x_pts):
    """Interpolate MRC onto x_pts (log-spaced); clamp outside range."""
    if len(xs) < 2:
        return np.full(len(x_pts), np.nan)
    return np.interp(x_pts, xs, ys, left=ys[0], right=ys[-1])


def load_snapshots(run_dir: Path):
    """Return list of (snapshot_index, xs, ys) sorted by index."""
    snaps = []
    for p in run_dir.iterdir():
        m = re.fullmatch(r'online_mrc\.bin\.(\d+)\.txt', p.name)
        if m:
            idx = int(m.group(1))
            xs, ys = read_shards(p)
            if len(xs) > 1:
                snaps.append((idx, xs, ys))
    snaps.sort(key=lambda t: t[0])
    return snaps


def compute_mae_series(run_dir: Path, gt_xs, gt_ys):
    """Compute MAE at each snapshot against GT."""
    x_pts = np.logspace(math.log10(X_MIN), math.log10(X_MAX), N_INTERP)
    gt_interp = interp_mrc(gt_xs, gt_ys, x_pts)

    snaps = load_snapshots(run_dir)
    indices, maes = [], []
    for idx, xs, ys in snaps:
        shards_interp = interp_mrc(xs, ys, x_pts)
        mask = ~np.isnan(shards_interp) & ~np.isnan(gt_interp)
        if mask.sum() == 0:
            continue
        mae = np.mean(np.abs(shards_interp[mask] - gt_interp[mask]))
        indices.append(idx)
        maes.append(mae)
    return np.array(indices), np.array(maes)


def draw_panel(ax, nd_dir, d_dir, gt_xs, gt_ys, title, phase_switch_snap):
    nd_idx, nd_mae = compute_mae_series(nd_dir, gt_xs, gt_ys)
    d_idx,  d_mae  = compute_mae_series(d_dir,  gt_xs, gt_ys)

    if len(nd_idx):
        ax.plot(nd_idx, nd_mae, color=COLOR_NO_DECAY, linewidth=1.5,
                linestyle='--', marker='o', markersize=3, zorder=3,
                label='No decay')
    if len(d_idx):
        ax.plot(d_idx, d_mae, color=COLOR_DECAY, linewidth=1.5,
                marker='o', markersize=3, zorder=4,
                label='Decay (λ=0.9)')

    if phase_switch_snap is not None:
        ax.axvline(phase_switch_snap, color='#888888', linewidth=1.0,
                   linestyle=':', zorder=2)
        ax.text(phase_switch_snap + 0.3, 0.96,
                'phase switch\n(est.)', fontsize=6.5, color='#666666', va='top',
                transform=ax.get_xaxis_transform())

    ax.set_xlabel('Snapshot index', fontsize=8)
    ax.set_ylabel('MAE (miss ratio)', fontsize=8)
    ax.set_title(title, fontsize=9, fontweight='bold', pad=4)
    ax.grid(True, linestyle=':', linewidth=0.4, color='#cccccc')
    ax.tick_params(labelsize=7)
    ax.set_ylim(bottom=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--forward-no-decay', type=Path, required=True)
    ap.add_argument('--forward-decay',    type=Path, required=True)
    ap.add_argument('--reverse-no-decay', type=Path, required=True)
    ap.add_argument('--reverse-decay',    type=Path, required=True)
    ap.add_argument('--zipfian-gt',       type=Path, required=True)
    ap.add_argument('--uniform-gt',       type=Path, required=True)
    ap.add_argument('--forward-switch-snap', type=float, default=None,
                    help='Snapshot index of phase switch for forward panel')
    ap.add_argument('--reverse-switch-snap', type=float, default=None,
                    help='Snapshot index of phase switch for reverse panel')
    ap.add_argument('--output', type=Path, default=Path('exp4_mae.png'))
    args = ap.parse_args()

    zip_gt_xs, zip_gt_ys = read_gt(args.zipfian_gt)
    uni_gt_xs, uni_gt_ys = read_gt(args.uniform_gt)

    fig, axes = plt.subplots(1, 2, figsize=(8, 3.5))
    fig.subplots_adjust(wspace=0.35)

    draw_panel(axes[0],
               args.forward_no_decay, args.forward_decay,
               zip_gt_xs, zip_gt_ys,
               '1c  Uniform → Zipfian: MAE over time',
               args.forward_switch_snap)
    draw_panel(axes[1],
               args.reverse_no_decay, args.reverse_decay,
               uni_gt_xs, uni_gt_ys,
               '1d  Zipfian → Uniform: MAE over time',
               args.reverse_switch_snap)

    nd_line = mlines.Line2D([], [], color=COLOR_NO_DECAY, linewidth=1.5,
                            linestyle='--', marker='o', markersize=3,
                            label='No decay')
    d_line  = mlines.Line2D([], [], color=COLOR_DECAY, linewidth=1.5,
                            marker='o', markersize=3, label='Decay (λ=0.9)')
    fig.legend(handles=[nd_line, d_line], loc='lower center',
               ncol=2, fontsize=8, framealpha=0.9,
               bbox_to_anchor=(0.5, 0.01))

    fig.savefig(args.output, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved: {args.output}')


if __name__ == '__main__':
    main()
