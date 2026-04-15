"""
exp2_mae_plot.py — MAE over time for each SHARDS sampling rate vs ground truth.

For each sampling rate, reads the SHARDS snapshot .txt files and computes MAE
against the final ground-truth MRC at each snapshot boundary.
Optionally also plots the GT snapshot MAE (how the LRU sim itself converges).

Outputs a single static HTML file with an embedded SVG line chart.

Usage:
  python3 exp2_mae_plot.py \\
    --results-dir    benchmark/online-mrc/results \\
    --truth-mrc      benchmark/online-mrc/results/old_experiments/exp2_trace/analysis/mrc.txt \\
    --shards-runs    exp2_shards_s1 exp2_shards_s01 exp2_shards_s001 exp2_shards_s0001 \\
    --labels         "s=1.0" "s=0.1" "s=0.01" "s=0.001" \\
    --avg-block-size 4080.2 \\
    [--gt-snapshots-dir benchmark/online-mrc/results/exp2_trace/analysis/gt_snapshots] \\
    --output         benchmark/online-mrc/results/exp2_analysis/exp2_mae.html
"""

import argparse
import math
from pathlib import Path


# ---------------------------------------------------------------------------
# Readers  (same logic as exp2_animation.py)
# ---------------------------------------------------------------------------

def read_shards_txt(path: Path, avg_bs: float):
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
            xs.append(cache_units * avg_bs)
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


def read_gt_snapshot(path: Path):
    """Read a gt_snapshot_N.txt file — same format as mrc.txt miss ratio curve section."""
    return read_ground_truth(path)


def collect_shards_snapshots(run_dir: Path, avg_bs: float):
    snaps = []
    for p in run_dir.iterdir():
        name = p.name
        if name.startswith('online_mrc.bin.') and name.endswith('.txt'):
            stem = name[len('online_mrc.bin.'):-len('.txt')]
            try:
                n = int(stem)
            except ValueError:
                continue
            xs, ys = read_shards_txt(p, avg_bs)
            snaps.append((n, xs, ys))
    snaps.sort(key=lambda s: s[0])
    return snaps


def collect_gt_snapshots(gt_dir: Path):
    snaps = []
    for p in gt_dir.iterdir():
        name = p.name
        if name.startswith('gt_snapshot_') and name.endswith('.txt'):
            stem = name[len('gt_snapshot_'):-len('.txt')]
            try:
                n = int(stem)
            except ValueError:
                continue
            xs, ys = read_gt_snapshot(p)
            snaps.append((n, xs, ys))
    snaps.sort(key=lambda s: s[0])
    return snaps


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


def compute_mae(xs, ys, truth_xs, truth_ys):
    errors = [abs(interpolate(xs, ys, tx) - ty)
              for tx, ty in zip(truth_xs, truth_ys)
              if interpolate(xs, ys, tx) is not None]
    return sum(errors) / len(errors) if errors else 0.0


# ---------------------------------------------------------------------------
# SVG chart
# ---------------------------------------------------------------------------

W, H = 860, 500
PAD_L, PAD_R, PAD_T, PAD_B = 80, 40, 50, 60
cw = W - PAD_L - PAD_R
ch = H - PAD_T - PAD_B

COLORS = ['#2196F3', '#4CAF50', '#FF9800', '#E91E63', '#9C27B0']
DASHES = ['', '6 3', '4 2', '2 2', '8 4']


def px(snap, x_min, x_max):
    if x_max == x_min:
        return PAD_L
    return PAD_L + (snap - x_min) / (x_max - x_min) * cw


def py(mae_pct, y_max):
    return PAD_T + (1.0 - mae_pct / y_max) * ch


def to_html(title, series, output_path: Path):
    """
    series: list of (label, snap_nums, mae_pcts, color, dash)
    """
    if not series:
        return

    all_snaps = [s for _, snaps, _, _, _ in series for s in snaps]
    all_maes  = [m for _, _, maes, _, _ in series for m in maes]
    x_min, x_max = min(all_snaps), max(all_snaps)
    y_max = max(all_maes) * 1.1 if all_maes else 1.0
    y_max = max(y_max, 1.0)

    L = []

    # SVG
    L.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
             f'font-family="monospace" font-size="12">')
    L.append(f'<rect width="{W}" height="{H}" fill="white"/>')
    L.append(f'<text x="{W//2}" y="30" text-anchor="middle" font-size="14" '
             f'font-weight="bold">{title}</text>')
    L.append(f'<rect x="{PAD_L}" y="{PAD_T}" width="{cw}" height="{ch}" '
             f'fill="none" stroke="#aaa" stroke-width="1"/>')

    # X grid + labels
    x_ticks = list(range(int(x_min), int(x_max) + 1, max(1, (int(x_max) - int(x_min)) // 8)))
    for xt in x_ticks:
        xg = px(xt, x_min, x_max)
        L.append(f'<line x1="{xg:.1f}" y1="{PAD_T}" x2="{xg:.1f}" y2="{PAD_T+ch}" '
                 f'stroke="#eee" stroke-dasharray="3 3"/>')
        L.append(f'<text x="{xg:.1f}" y="{PAD_T+ch+18}" text-anchor="middle" '
                 f'font-size="10" fill="#666">{xt}</text>')
    L.append(f'<text x="{PAD_L+cw//2}" y="{H-8}" text-anchor="middle" '
             f'font-size="11" fill="#333">Snapshot (×{1000000:,} accesses)</text>')

    # Y grid + labels
    n_yticks = 5
    for i in range(n_yticks + 1):
        mv = y_max * i / n_yticks
        yg = py(mv, y_max)
        L.append(f'<line x1="{PAD_L}" y1="{yg:.1f}" x2="{PAD_L+cw}" y2="{yg:.1f}" '
                 f'stroke="#eee" stroke-dasharray="3 3"/>')
        L.append(f'<text x="{PAD_L-5}" y="{yg+4:.1f}" text-anchor="end" '
                 f'font-size="10" fill="#666">{mv:.1f}%</text>')
    rot_x, rot_y = 14, PAD_T + ch // 2
    L.append(f'<text x="{rot_x}" y="{rot_y}" text-anchor="middle" font-size="11" '
             f'fill="#333" transform="rotate(-90,{rot_x},{rot_y})">MAE vs final ground truth</text>')

    # Series lines
    for label, snaps, maes, color, dash in series:
        if not snaps:
            continue
        pts = " ".join(f"{px(s,x_min,x_max):.1f},{py(m,y_max):.1f}"
                       for s, m in zip(snaps, maes))
        dash_attr = f'stroke-dasharray="{dash}"' if dash else ''
        L.append(f'<polyline points="{pts}" fill="none" stroke="{color}" '
                 f'stroke-width="2" {dash_attr}/>')
        # Dots
        for s, m in zip(snaps, maes):
            L.append(f'<circle cx="{px(s,x_min,x_max):.1f}" cy="{py(m,y_max):.1f}" '
                     f'r="3" fill="{color}"/>')

    # Legend
    lx, ly = PAD_L + 20, PAD_T + 15
    for label, _, _, color, dash in series:
        dash_attr = f'stroke-dasharray="{dash}"' if dash else ''
        L.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+25}" y2="{ly}" '
                 f'stroke="{color}" stroke-width="2" {dash_attr}/>')
        L.append(f'<text x="{lx+30}" y="{ly+4}" font-size="11" fill="#333">{label}</text>')
        ly += 18

    L.append('</svg>')

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{title}</title>
<style>body{{font-family:monospace;margin:20px;background:#f5f5f5;}}
svg{{background:white;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.12);}}</style>
</head>
<body>
<h2>{title}</h2>
{''.join(L)}
</body></html>"""

    output_path.write_text(html)
    print(f"Saved: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results-dir',      type=Path, required=True)
    ap.add_argument('--truth-mrc',        type=Path, required=True)
    ap.add_argument('--shards-runs',      type=str, nargs='+',
                    default=['exp2_shards_s1','exp2_shards_s01',
                             'exp2_shards_s001','exp2_shards_s0001'])
    ap.add_argument('--labels',           type=str, nargs='+',
                    default=['s=1.0','s=0.1','s=0.01','s=0.001'])
    ap.add_argument('--avg-block-size',   type=float, default=4080.2)
    ap.add_argument('--gt-snapshots-dir', type=Path, default=None)
    ap.add_argument('--output',           type=Path, required=True)
    args = ap.parse_args()

    print(f"Reading ground truth from {args.truth_mrc} ...")
    truth_xs, truth_ys = read_ground_truth(args.truth_mrc)
    print(f"  {len(truth_xs)} points")

    labels = list(args.labels)
    while len(labels) < len(args.shards_runs):
        labels.append(args.shards_runs[len(labels)])

    series = []

    # SHARDS series
    for run_name, label, color in zip(args.shards_runs, labels, COLORS):
        run_dir = args.results_dir / run_name
        print(f"\n[{label}] {run_dir}")
        snaps = collect_shards_snapshots(run_dir, args.avg_block_size)
        print(f"  {len(snaps)} snapshots")
        if not snaps:
            continue
        snap_nums = [n for n, _, _ in snaps]
        maes = [compute_mae(xs, ys, truth_xs, truth_ys) * 100
                for _, xs, ys in snaps]
        series.append((label, snap_nums, maes, color, ''))

    # GT snapshot series (optional)
    if args.gt_snapshots_dir and args.gt_snapshots_dir.exists():
        print(f"\n[GT snapshots] {args.gt_snapshots_dir}")
        gt_snaps = collect_gt_snapshots(args.gt_snapshots_dir)
        print(f"  {len(gt_snaps)} snapshots")
        if gt_snaps:
            snap_nums = [n for n, _, _ in gt_snaps]
            maes = [compute_mae(xs, ys, truth_xs, truth_ys) * 100
                    for _, xs, ys in gt_snaps]
            series.append(('Ground truth (LRU)', snap_nums, maes, '#FF9800', '6 3'))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    to_html("Exp2: MAE vs Ground Truth over Time", series, args.output)


if __name__ == '__main__':
    main()
