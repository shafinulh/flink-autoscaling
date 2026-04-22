"""
exp4_decay_png.py — compare no-decay vs decay SHARDS on distribution-shift workloads.

Two panels:
  Left:  forward switch (uniform→Zipfian) — no decay vs decay vs Zipfian GT
  Right: reverse switch (Zipfian→uniform) — no decay vs decay vs uniform GT

Usage:
  python3 exp4_decay_png.py \\
    --forward-no-decay  results/exp3_forward_switch/online_mrc.bin.txt \\
    --forward-decay     results/exp4_forward_decay/online_mrc.bin.txt \\
    --reverse-no-decay  results/exp3_reverse_switch/online_mrc.bin.txt \\
    --reverse-decay     results/exp4_reverse_decay/online_mrc.bin.txt \\
    --zipfian-gt        results/old_experiments/.../exp2_trace/analysis/mrc.txt \\
    --uniform-gt        results/exp3_uniform_trace/analysis/mrc.txt \\
    --output            final_report/exp4_decay.png
"""

import argparse
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.lines as mlines

AVG_BS   = 4080.0
SAMPLING = 0.01
X_MIN    = 1  * 1024 ** 2
X_MAX    = 16 * 1024 ** 3

COLOR_NO_DECAY = '#d62728'   # red
COLOR_DECAY    = '#2ca02c'   # green
COLOR_GT       = 'black'


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
    return xs, ys


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
    return xs, ys


def clip(xs, ys):
    pairs = [(x, y) for x, y in zip(xs, ys) if X_MIN <= x <= X_MAX]
    if not pairs:
        return [], []
    return zip(*pairs)


def draw_panel(ax, no_decay_path, decay_path, gt_path, title, subtitle):
    sx_nd, sy_nd = read_shards(no_decay_path)
    sx_d,  sy_d  = read_shards(decay_path)
    gx, gy       = read_gt(gt_path)

    sx_nd, sy_nd = [list(v) for v in clip(sx_nd, sy_nd)]
    sx_d,  sy_d  = [list(v) for v in clip(sx_d,  sy_d)]
    gx, gy       = [list(v) for v in clip(gx, gy)]

    if sx_nd:
        ax.plot(sx_nd, sy_nd, color=COLOR_NO_DECAY, linewidth=1.5,
                linestyle='--', zorder=3, label='No decay')
    if sx_d:
        ax.plot(sx_d, sy_d, color=COLOR_DECAY, linewidth=1.5,
                zorder=4, label='Decay (λ=0.9)')
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
        fontsize=7)
    ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['0.0', '0.2', '0.4', '0.6', '0.8', '1.0'], fontsize=7)
    ax.grid(True, which='both', linestyle=':', linewidth=0.4, color='#cccccc')
    ax.set_title(title, fontsize=9, fontweight='bold', pad=4)
    ax.text(0.5, 0.97, subtitle, transform=ax.transAxes, fontsize=7.5,
            ha='center', va='top', style='italic')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--forward-no-decay', type=Path, required=True)
    ap.add_argument('--forward-decay',    type=Path, required=True)
    ap.add_argument('--reverse-no-decay', type=Path, required=True)
    ap.add_argument('--reverse-decay',    type=Path, required=True)
    ap.add_argument('--zipfian-gt',       type=Path, required=True)
    ap.add_argument('--uniform-gt',       type=Path, required=True)
    ap.add_argument('--output',           type=Path, default=Path('exp4_decay.png'))
    args = ap.parse_args()

    fig, axes = plt.subplots(1, 2, figsize=(8, 3.5))
    fig.subplots_adjust(wspace=0.3)

    draw_panel(axes[0],
               args.forward_no_decay, args.forward_decay, args.zipfian_gt,
               '1c  Uniform → Zipfian (20M + 20M)',
               'Effect of decay on contaminated MRC')
    draw_panel(axes[1],
               args.reverse_no_decay, args.reverse_decay, args.uniform_gt,
               '1d  Zipfian → Uniform (20M + 20M)',
               'Effect of decay on contaminated MRC')

    axes[0].set_ylabel('Miss ratio', fontsize=8)
    for ax in axes:
        ax.set_xlabel('Cache size', fontsize=8)

    nd_line = mlines.Line2D([], [], color=COLOR_NO_DECAY, linewidth=1.5,
                            linestyle='--', label='No decay')
    d_line  = mlines.Line2D([], [], color=COLOR_DECAY,    linewidth=1.5,
                            label='Decay (λ=0.9)')
    gt_line = mlines.Line2D([], [], color=COLOR_GT,       linewidth=1.2,
                            linestyle='--', marker='o', markersize=3,
                            label='Ground truth')
    fig.legend(handles=[nd_line, d_line, gt_line], loc='lower center',
               ncol=3, fontsize=8, framealpha=0.9,
               bbox_to_anchor=(0.5, 0.01))

    fig.savefig(args.output, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved: {args.output}')


if __name__ == '__main__':
    main()
