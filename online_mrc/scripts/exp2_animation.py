"""
exp2_animation.py — Experiment 2 SHARDS MRC convergence animation.

Generates one HTML file per sampling rate showing the MRC evolving
snapshot by snapshot toward the ground truth, with MAE displayed live.

Usage:
  python3 exp2_animation.py \\
    --results-dir /path/to/benchmark/online-mrc/results \\
    --truth-run   exp2_trace \\
    --shards-runs exp2_shards_s1 exp2_shards_s01 exp2_shards_s001 exp2_shards_s0001 \\
    --labels "s=1.0" "s=0.1" "s=0.01" "s=0.001" \\
    --filtered-csv /tom/distr_project/exp2_trace/analysis/filtered.csv \\
    --interval 1000000 \\
    --output-dir /path/to/output
"""

import argparse
import json
import math
from pathlib import Path


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_shards_txt(path: Path, avg_bs: float):
    """Read a SHARDS .txt snapshot. Returns (bytes_list, miss_ratio_list)."""
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
    """Read ground truth mrc.txt. Returns (bytes_list, miss_ratio_0to1_list)."""
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


def avg_block_size_from_csv(path: Path, max_entries: int = 2_000_000) -> float:
    """Avg block size from filtered.csv (already type=9 only). Col 3 = block_size."""
    total, count = 0, 0
    with open(path) as f:
        for line in f:
            if count >= max_entries:
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) < 4:
                continue
            try:
                size = int(parts[3])
                if size > 0:
                    total += size
                    count += 1
            except ValueError:
                continue
    if count == 0:
        raise ValueError(f"No valid entries in {path}")
    avg = total / count
    print(f"  Avg block size: {avg:.1f} B  ({count:,} entries sampled)")
    return avg


def collect_snapshots(run_dir: Path, avg_bs: float):
    """
    Collect all online_mrc.bin.N.txt snapshot files, sorted by N.
    Returns list of (snap_num, xs, ys).
    """
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


# ---------------------------------------------------------------------------
# SVG / plot helpers
# ---------------------------------------------------------------------------

W, H = 960, 580
PAD_L, PAD_R, PAD_T, PAD_B = 90, 40, 55, 70
plot_w = W - PAD_L - PAD_R
plot_h = H - PAD_T - PAD_B


def _px(b, log_min, log_max):
    if b <= 0:
        return PAD_L
    return PAD_L + (math.log10(b) - log_min) / (log_max - log_min) * plot_w


def _py(mr):
    return PAD_T + (1.0 - max(0.0, min(1.0, mr))) * plot_h


def _fmt_bytes(b):
    for unit, thr in [("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)]:
        if b >= thr:
            v = b / thr
            return f"{v:.0f}{unit}" if v == int(v) else f"{v:.1f}{unit}"
    return f"{b:.0f}B"


def _static_svg(truth_xs, truth_ys, log_min, log_max, label):
    """Return SVG markup for all static elements (axes, grid, ground truth, legend)."""
    L = []

    L.append(f'<rect width="{W}" height="{H}" fill="white"/>')
    L.append(
        f'<text x="{W//2}" y="32" text-anchor="middle" '
        f'font-size="15" font-weight="bold" font-family="monospace">'
        f'SHARDS MRC Convergence — {label}</text>'
    )
    L.append(
        f'<rect x="{PAD_L}" y="{PAD_T}" width="{plot_w}" height="{plot_h}" '
        f'fill="none" stroke="#aaa" stroke-width="1"/>'
    )

    # X grid + labels (powers of 10)
    for p in range(int(math.ceil(log_min)), int(math.floor(log_max)) + 1):
        xg = _px(10**p, log_min, log_max)
        L.append(f'<line x1="{xg:.1f}" y1="{PAD_T}" x2="{xg:.1f}" y2="{PAD_T+plot_h}" stroke="#eee" stroke-dasharray="4 4"/>')
        L.append(
            f'<text x="{xg:.1f}" y="{PAD_T+plot_h+18}" text-anchor="middle" '
            f'font-size="11" fill="#666" font-family="monospace">{_fmt_bytes(10**p)}</text>'
        )

    # Y grid + labels
    for tick in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
        yg = _py(tick)
        L.append(f'<line x1="{PAD_L}" y1="{yg:.1f}" x2="{PAD_L+plot_w}" y2="{yg:.1f}" stroke="#eee" stroke-dasharray="4 4"/>')
        L.append(
            f'<text x="{PAD_L-6}" y="{yg+4:.1f}" text-anchor="end" '
            f'font-size="11" fill="#666" font-family="monospace">{tick:.1f}</text>'
        )

    # Axis labels
    L.append(
        f'<text x="{PAD_L+plot_w//2}" y="{H-8}" text-anchor="middle" '
        f'font-size="12" fill="#333" font-family="monospace">Cache capacity (log scale)</text>'
    )
    cx, cy = 14, PAD_T + plot_h // 2
    L.append(
        f'<text x="{cx}" y="{cy}" text-anchor="middle" font-size="12" fill="#333" '
        f'font-family="monospace" transform="rotate(-90,{cx},{cy})">Miss ratio</text>'
    )

    # Ground truth
    valid = [(b, m) for b, m in zip(truth_xs, truth_ys) if b > 0]
    if len(valid) >= 2:
        pts = " ".join(f"{_px(b,log_min,log_max):.1f},{_py(m):.1f}" for b, m in valid)
        L.append(f'<polyline points="{pts}" fill="none" stroke="#f90" stroke-width="2.5" stroke-linejoin="round"/>')
    for b, m in valid:
        L.append(
            f'<circle cx="{_px(b,log_min,log_max):.1f}" cy="{_py(m):.1f}" '
            f'r="4" fill="#f90" stroke="white" stroke-width="1"/>'
        )

    # Legend
    lx, ly = PAD_L + 20, PAD_T + 20
    L.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+30}" y2="{ly}" stroke="steelblue" stroke-width="2.5"/>')
    L.append(f'<text x="{lx+36}" y="{ly+4}" font-size="12" fill="#333" font-family="monospace">SHARDS (current snapshot)</text>')
    ly2 = ly + 22
    L.append(f'<line x1="{lx}" y1="{ly2}" x2="{lx+30}" y2="{ly2}" stroke="#f90" stroke-width="2.5"/>')
    L.append(f'<circle cx="{lx+15}" cy="{ly2}" r="4" fill="#f90"/>')
    L.append(f'<text x="{lx+36}" y="{ly2+4}" font-size="12" fill="#333" font-family="monospace">Ground truth (LRU sim)</text>')

    return "\n  ".join(L)


def _shards_path(xs, ys, log_min, log_max):
    """SVG step-path string for a SHARDS MRC."""
    valid = [(b, m) for b, m in zip(xs, ys) if b > 0 and 0.0 <= m <= 1.0]
    if not valid:
        return ""
    pts = []
    for i, (b, m) in enumerate(valid):
        xp = _px(b, log_min, log_max)
        yp = _py(m)
        pts.append(f"M{xp:.1f},{yp:.1f}" if i == 0 else f"H{xp:.1f}V{yp:.1f}")
    return " ".join(pts)


def _interpolate(xs, ys, target):
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


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def make_html(snapshots, truth_xs, truth_ys, label, interval, out_path: Path):
    n = len(snapshots)

    # X range: union of ground truth and all snapshot data
    all_x = [b for b in truth_xs if b > 0]
    for _, xs, _ in snapshots:
        all_x.extend(x for x in xs if x > 0)
    log_min = math.log10(min(all_x))
    log_max = math.log10(max(all_x))

    static = _static_svg(truth_xs, truth_ys, log_min, log_max, label)

    # Precompute path strings and MAEs
    paths, maes, snap_nums = [], [], []
    for snap_num, xs, ys in snapshots:
        paths.append(_shards_path(xs, ys, log_min, log_max))
        errors = [abs(_interpolate(xs, ys, tb) - tmr)
                  for tb, tmr in zip(truth_xs, truth_ys)
                  if _interpolate(xs, ys, tb) is not None]
        maes.append(round(sum(errors) / len(errors), 4) if errors else 0.0)
        snap_nums.append(snap_num)

    paths_js    = json.dumps(paths)
    maes_js     = json.dumps(maes)
    snap_nums_js = json.dumps(snap_nums)
    last_snap   = snap_nums[-1]

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SHARDS Animation — {label}</title>
<style>
  body    {{ font-family: monospace; margin: 20px; background: #f5f5f5; }}
  h2      {{ margin-bottom: 10px; }}
  #controls {{
    display: flex; align-items: center; gap: 20px;
    padding: 10px 18px; margin-bottom: 12px;
    background: white; border-radius: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,.12);
  }}
  button  {{ padding: 6px 20px; font-size: 14px; font-family: monospace;
             cursor: pointer; border: 1px solid #aaa; border-radius: 4px;
             background: #fff; }}
  button:hover {{ background: #eee; }}
  #slider {{ width: 320px; cursor: pointer; }}
  .info   {{ font-size: 13px; line-height: 1.8; }}
  .mae    {{ color: #666; }}
  select  {{ font-family: monospace; font-size: 13px; padding: 3px 8px; }}
  svg     {{ background: white; border-radius: 6px;
             box-shadow: 0 1px 4px rgba(0,0,0,.12); display: block; }}
</style>
</head>
<body>
<h2>SHARDS MRC Convergence — {label}</h2>
<div id="controls">
  <button id="btn">&#9654; Play</button>
  <input type="range" id="slider" min="0" max="{n-1}" value="0" step="1">
  <div class="info">
    <div id="snap-info">Snapshot 1 / {last_snap} &nbsp;|&nbsp; 1M accesses seen</div>
    <div id="mae-info" class="mae">MAE vs ground truth: —</div>
  </div>
  <label>Speed:
    <select id="speed">
      <option value="1000">0.5&#215;</option>
      <option value="600" selected>1&#215;</option>
      <option value="300">2&#215;</option>
      <option value="120">5&#215;</option>
      <option value="60">10&#215;</option>
    </select>
  </label>
</div>

<svg width="{W}" height="{H}" xmlns="http://www.w3.org/2000/svg">
  {static}
  <path id="shards-path" d="" fill="none" stroke="steelblue"
        stroke-width="2" stroke-linejoin="round"/>
</svg>

<script>
const paths    = {paths_js};
const maes     = {maes_js};
const snapNums = {snap_nums_js};
const INTERVAL = {interval};

let cur     = 0;
let playing = false;
let timer   = null;

const btn      = document.getElementById('btn');
const slider   = document.getElementById('slider');
const snapInfo = document.getElementById('snap-info');
const maeInfo  = document.getElementById('mae-info');
const speedSel = document.getElementById('speed');
const pathEl   = document.getElementById('shards-path');

function fmtNum(n) {{
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(0) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(0) + 'K';
  return String(n);
}}

function render(i) {{
  pathEl.setAttribute('d', paths[i] || '');
  const sn = snapNums[i];
  snapInfo.textContent =
    `Snapshot ${{sn}} / ${{snapNums[snapNums.length-1]}}  |  ${{fmtNum(sn * INTERVAL)}} accesses seen`;
  maeInfo.textContent = `MAE vs ground truth: ${{(maes[i]*100).toFixed(2)}}%`;
  slider.value = i;
}}

function step() {{
  if (cur >= paths.length - 1) {{
    playing = false;
    btn.innerHTML = '&#9654; Play';
    clearInterval(timer);
    return;
  }}
  render(++cur);
}}

btn.addEventListener('click', () => {{
  if (playing) {{
    playing = false;
    btn.innerHTML = '&#9654; Play';
    clearInterval(timer);
  }} else {{
    if (cur >= paths.length - 1) cur = 0;
    playing = true;
    btn.innerHTML = '&#9646;&#9646; Pause';
    timer = setInterval(step, parseInt(speedSel.value));
  }}
}});

slider.addEventListener('input', () => {{ render(cur = +slider.value); }});

speedSel.addEventListener('change', () => {{
  if (playing) {{ clearInterval(timer); timer = setInterval(step, parseInt(speedSel.value)); }}
}});

render(0);
</script>
</body>
</html>"""

    out_path.write_text(html)
    print(f"  Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--results-dir',    type=Path, required=True,
                    help='Parent directory containing all run subdirectories')
    ap.add_argument('--truth-run',      type=str,  default='exp2_trace',
                    help='Run ID for the ground truth trace')
    ap.add_argument('--shards-runs',    type=str,  nargs='+',
                    default=['exp2_shards_s1','exp2_shards_s01',
                             'exp2_shards_s001','exp2_shards_s0001'])
    ap.add_argument('--labels',         type=str,  nargs='+',
                    default=['s=1.0','s=0.1','s=0.01','s=0.001'])
    ap.add_argument('--avg-block-size', type=float, default=None,
                    help='Override avg block size in bytes (skips CSV computation)')
    ap.add_argument('--filtered-csv',   type=Path,  default=None,
                    help='Path to filtered.csv for avg block size (default: <truth-run>/analysis/filtered.csv)')
    ap.add_argument('--interval',       type=int,   default=1_000_000,
                    help='ROCKSDB_SHARDS_INTERVAL used during the runs')
    ap.add_argument('--output-dir',     type=Path,  default=None,
                    help='Where to write HTML files (default: <results-dir>/exp2_analysis/)')
    args = ap.parse_args()

    out_dir = args.output_dir or (args.results_dir / 'exp2_analysis')
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- avg block size ---
    if args.avg_block_size:
        avg_bs = args.avg_block_size
        print(f"Using provided avg block size: {avg_bs:.1f} B")
    else:
        csv_path = (args.filtered_csv or
                    args.results_dir / args.truth_run / 'analysis' / 'filtered.csv')
        print(f"Computing avg block size from {csv_path} ...")
        avg_bs = avg_block_size_from_csv(csv_path)

    # --- ground truth ---
    truth_path = args.results_dir / args.truth_run / 'analysis' / 'mrc.txt'
    print(f"Reading ground truth from {truth_path} ...")
    truth_xs, truth_ys = read_ground_truth(truth_path)
    print(f"  {len(truth_xs)} ground truth points")

    # --- one HTML per sampling rate ---
    labels = list(args.labels)
    while len(labels) < len(args.shards_runs):
        labels.append(args.shards_runs[len(labels)])

    for run_name, label in zip(args.shards_runs, labels):
        run_dir = args.results_dir / run_name
        print(f"\n[{label}]  {run_dir}")
        snaps = collect_snapshots(run_dir, avg_bs)
        print(f"  {len(snaps)} snapshots found")
        if not snaps:
            print("  WARNING: no snapshots — skipping")
            continue
        safe = label.replace('=', '').replace('.', '')
        make_html(snaps, truth_xs, truth_ys, label, args.interval,
                  out_dir / f'exp2_animation_{safe}.html')

    print(f"\nAll done. Output: {out_dir}/")


if __name__ == '__main__':
    main()
