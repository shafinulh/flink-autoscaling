"""
exp3_distribution_shift_png.py — 4-panel distribution shift figure for Exp3.

Shows that SHARDS works correctly on stable workloads (panels 1a, 1b) but
produces a contaminated MRC after a distribution shift (panels 1c, 1d).

  1a  Uniform 20M ops       → SHARDS ≈ uniform GT
  1b  Zipfian 20M ops       → SHARDS ≈ Zipfian GT
  1c  Uniform→Zipfian 40M   → SHARDS final ≠ Zipfian GT (uniform history contaminates)
  1d  Zipfian→Uniform 40M   → SHARDS final ≠ uniform GT (Zipfian history contaminates)

1b reuses exp2 data (s=0.01). 1c uses the Zipfian GT from 1b as reference.
1d uses the uniform GT from 1a as reference.

Usage:
  python3 exp3_distribution_shift_png.py \\
    --uniform-shards   results/exp3_uniform_shards/online_mrc.bin.txt \\
    --uniform-gt       results/exp3_uniform_trace/analysis/mrc.txt \\
    --zipfian-shards   results/old_experiments/.../exp2_shards_s001/online_mrc.bin.txt \\
    --zipfian-gt       results/old_experiments/.../exp2_trace/analysis/mrc.txt \\
    --forward-shards   results/exp3_forward_switch/online_mrc.bin.txt \\
    --reverse-shards   results/exp3_reverse_switch/online_mrc.bin.txt \\
    --output           /path/to/final_report/exp3_distribution_shift.png
"""

import argparse
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np

AVG_BS   = 4080.0
SAMPLING = 0.01
X_MIN    = 1  * 1024 ** 2   # 1 MB
X_MAX    = 16 * 1024 ** 3   # 16 GiB

COLOR_GOOD = '#1f77b4'   # blue  — panels where SHARDS matches GT
COLOR_BAD  = '#d62728'   # red   — panels where SHARDS is contaminated
COLOR_GT   = 'black'


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_shards(path: Path, sampling: float = SAMPLING):
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
            xs.append(cache_units * AVG_BS / sampling)
            ys.append(miss_ratio)
    return xs, ys


def read_gt(path: Path):
    """Parse mrc.txt produced by gen_ground_truth_mrc.sh."""
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
            if len(parts) < 5:
                continue
            try:
                cap = float(parts[3])
                mr  = float(parts[4]) / 100.0
            except ValueError:
                continue
            xs.append(cap)
            ys.append(mr)
    return xs, ys


def clip(xs, ys):
    pairs = [(x, y) for x, y in zip(xs, ys) if X_MIN <= x <= X_MAX]
    if not pairs:
        return [], []
    return zip(*pairs)


# ---------------------------------------------------------------------------
# Single panel
# ---------------------------------------------------------------------------

def draw_panel(ax, shards_path, gt_path, title, subtitle, shards_color,
               sampling=SAMPLING):
    sx, sy = read_shards(shards_path, sampling)
    gx, gy = read_gt(gt_path)

    sx, sy = [list(v) for v in clip(sx, sy)]
    gx, gy = [list(v) for v in clip(gx, gy)]

    if sx:
        ax.plot(sx, sy, color=shards_color, linewidth=1.5, zorder=3,
                label='SHARDS (1% sampling)')
    if gx:
        ax.plot(gx, gy, color=COLOR_GT, linewidth=1.2, linestyle='--',
                marker='o', markersize=2.5, zorder=2, label='Ground truth')

    ax.set_xscale('log')
    ax.set_xlim(X_MIN, X_MAX)
    ax.set_ylim(-0.02, 1.05)

    p2_start = math.ceil(math.log2(X_MIN))
    p2_end   = math.floor(math.log2(X_MAX))
    ticks    = [2**p for p in range(p2_start, p2_end + 1)]
    ax.set_xticks(ticks)

    def fmt(b):
        if b >= 1 << 30: return f'{b >> 30}GB'
        if b >= 1 << 20: return f'{b >> 20}MB'
        return ''

    ax.set_xticklabels(
        [fmt(t) if math.log2(t) % 2 == 0 else '' for t in ticks],
        fontsize=7,
    )
    ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['0.0', '0.2', '0.4', '0.6', '0.8', '1.0'], fontsize=7)
    ax.grid(True, which='both', linestyle=':', linewidth=0.4, color='#cccccc')

    ax.set_title(title, fontsize=9, fontweight='bold', pad=4)
    ax.text(0.5, 0.97, subtitle, transform=ax.transAxes, fontsize=7.5,
            ha='center', va='top',
            color=shards_color,
            style='italic')


# ---------------------------------------------------------------------------
# Main figure
# ---------------------------------------------------------------------------

def make_figure(args):
    fig, axes = plt.subplots(2, 2, figsize=(8, 6))
    fig.subplots_adjust(hspace=0.38, wspace=0.28)

    panels = [
        (axes[0, 0], args.uniform_shards, args.uniform_gt,
         '1a  Uniform (20M ops)',
         'SHARDS ≈ GT',
         COLOR_GOOD),
        (axes[0, 1], args.zipfian_shards, args.zipfian_gt,
         '1b  Zipfian (20M ops)',
         'SHARDS ≈ GT',
         COLOR_GOOD),
        (axes[1, 0], args.forward_shards, args.zipfian_gt,
         '1c  Uniform → Zipfian (20M + 20M)',
         'SHARDS ≠ Zipfian GT  (contaminated)',
         COLOR_BAD),
        (axes[1, 1], args.reverse_shards, args.uniform_gt,
         '1d  Zipfian → Uniform (20M + 20M)',
         'SHARDS ≠ Uniform GT  (contaminated)',
         COLOR_BAD),
    ]

    for ax, shards_path, gt_path, title, subtitle, color in panels:
        draw_panel(ax, shards_path, gt_path, title, subtitle, color,
                   sampling=args.sampling)

    # Shared axis labels
    for ax in axes[:, 0]:
        ax.set_ylabel('Miss ratio', fontsize=8)
    for ax in axes[1, :]:
        ax.set_xlabel('Cache size', fontsize=8)

    # Single shared legend at the bottom
    shards_line = mlines.Line2D([], [], color='gray', linewidth=1.5,
                                label='SHARDS (1% sampling)')
    gt_line     = mlines.Line2D([], [], color=COLOR_GT, linewidth=1.2,
                                linestyle='--', marker='o', markersize=3,
                                label='Ground truth')
    fig.legend(handles=[shards_line, gt_line], loc='lower center',
               ncol=2, fontsize=8, framealpha=0.9,
               bbox_to_anchor=(0.5, 0.01))

    fig.savefig(args.output, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved: {args.output}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--uniform-shards',  type=Path, required=True)
    ap.add_argument('--uniform-gt',      type=Path, required=True)
    ap.add_argument('--zipfian-shards',  type=Path, required=True)
    ap.add_argument('--zipfian-gt',      type=Path, required=True)
    ap.add_argument('--forward-shards',  type=Path, required=True)
    ap.add_argument('--reverse-shards',  type=Path, required=True)
    ap.add_argument('--sampling',        type=float, default=0.01)
    ap.add_argument('--output',          type=Path,
                    default=Path('exp3_distribution_shift.png'))
    args = ap.parse_args()
    make_figure(args)


if __name__ == '__main__':
    main()
