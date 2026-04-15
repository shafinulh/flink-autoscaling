"""
exp2_animation.py — Experiment 2 SHARDS MRC convergence animation.

Generates a single HTML file with 4 side-by-side panels (one per sampling rate),
all driven by shared playback controls. Each panel shows the SHARDS MRC evolving
snapshot-by-snapshot toward the ground truth, with live MAE.

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
# Panel drawing helpers
# ---------------------------------------------------------------------------

# Per-panel SVG dimensions
PW, PH = 400, 400
PP_L, PP_R, PP_T, PP_B = 65, 15, 50, 60
pw = PW - PP_L - PP_R   # 320
ph = PH - PP_T - PP_B   # 290


def _px(b, log_min, log_max):
    if b <= 0:
        return PP_L
    return PP_L + (math.log10(b) - log_min) / (log_max - log_min) * pw


def _py(mr):
    return PP_T + (1.0 - max(0.0, min(1.0, mr))) * ph


def _fmt_bytes(b):
    for unit, thr in [("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)]:
        if b >= thr:
            v = b / thr
            return f"{v:.0f}{unit}" if v == int(v) else f"{v:.1f}{unit}"
    return f"{b:.0f}B"


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


def _panel_svg(panel_idx, truth_xs, truth_ys, log_min, log_max, label):
    """Return SVG markup for one panel's static elements (axes, grid, ground truth, legend)."""
    L = []

    L.append(f'<rect width="{PW}" height="{PH}" fill="white"/>')

    # Panel title (sampling rate label)
    L.append(
        f'<text x="{PW//2}" y="28" text-anchor="middle" '
        f'font-size="13" font-weight="bold" font-family="monospace">{label}</text>'
    )
    L.append(
        f'<rect x="{PP_L}" y="{PP_T}" width="{pw}" height="{ph}" '
        f'fill="none" stroke="#aaa" stroke-width="1"/>'
    )

    # X grid + labels (powers of 10)
    for p in range(int(math.ceil(log_min)), int(math.floor(log_max)) + 1):
        xg = _px(10**p, log_min, log_max)
        L.append(f'<line x1="{xg:.1f}" y1="{PP_T}" x2="{xg:.1f}" y2="{PP_T+ph}" stroke="#eee" stroke-dasharray="3 3"/>')
        L.append(
            f'<text x="{xg:.1f}" y="{PP_T+ph+15}" text-anchor="middle" '
            f'font-size="9" fill="#666" font-family="monospace">{_fmt_bytes(10**p)}</text>'
        )

    # Y grid + labels (only on leftmost panel)
    for tick in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]:
        yg = _py(tick)
        L.append(f'<line x1="{PP_L}" y1="{yg:.1f}" x2="{PP_L+pw}" y2="{yg:.1f}" stroke="#eee" stroke-dasharray="3 3"/>')
        if panel_idx == 0:
            L.append(
                f'<text x="{PP_L-5}" y="{yg+4:.1f}" text-anchor="end" '
                f'font-size="10" fill="#666" font-family="monospace">{tick:.1f}</text>'
            )

    # Axis labels
    L.append(
        f'<text x="{PP_L+pw//2}" y="{PH-6}" text-anchor="middle" '
        f'font-size="10" fill="#333" font-family="monospace">Cache size (log scale)</text>'
    )
    if panel_idx == 0:
        cx, cy = 11, PP_T + ph // 2
        L.append(
            f'<text x="{cx}" y="{cy}" text-anchor="middle" font-size="10" fill="#333" '
            f'font-family="monospace" transform="rotate(-90,{cx},{cy})">Miss ratio</text>'
        )

    # Ground truth line + dots
    valid = [(b, m) for b, m in zip(truth_xs, truth_ys) if b > 0]
    if len(valid) >= 2:
        pts = " ".join(f"{_px(b,log_min,log_max):.1f},{_py(m):.1f}" for b, m in valid)
        L.append(f'<polyline points="{pts}" fill="none" stroke="#f90" stroke-width="2" stroke-linejoin="round"/>')
    for b, m in valid:
        L.append(
            f'<circle cx="{_px(b,log_min,log_max):.1f}" cy="{_py(m):.1f}" '
            f'r="3" fill="#f90" stroke="white" stroke-width="1"/>'
        )

    # Legend (inside panel, top-right)
    lx = PP_L + pw - 160
    ly = PP_T + 12
    L.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+20}" y2="{ly}" stroke="steelblue" stroke-width="2"/>')
    L.append(f'<text x="{lx+24}" y="{ly+4}" font-size="9" fill="#333" font-family="monospace">SHARDS</text>')
    ly2 = ly + 16
    L.append(f'<line x1="{lx}" y1="{ly2}" x2="{lx+20}" y2="{ly2}" stroke="#f90" stroke-width="2"/>')
    L.append(f'<circle cx="{lx+10}" cy="{ly2}" r="3" fill="#f90"/>')
    L.append(f'<text x="{lx+24}" y="{ly2+4}" font-size="9" fill="#333" font-family="monospace">Ground truth</text>')

    # MAE text (updated by JS)
    L.append(
        f'<text id="mae-{panel_idx}" x="{PP_L+pw//2}" y="{PP_T+ph+32}" '
        f'text-anchor="middle" font-size="10" fill="#555" font-family="monospace">MAE: —</text>'
    )

    return "\n    ".join(L)


def _shards_path(xs, ys, log_min, log_max):
    """SVG path string for a SHARDS MRC (straight lines, spanning full x range)."""
    valid = [(b, m) for b, m in zip(xs, ys) if b > 0 and 0.0 <= m <= 1.0]
    if not valid:
        return ""
    # Prepend: miss ratio is 1.0 for any cache smaller than the first tracked bin
    x_min = 10 ** log_min
    if valid[0][0] > x_min:
        valid = [(x_min, 1.0)] + valid
    # Extend last point horizontally to right edge of plot
    x_max = 10 ** log_max
    if valid[-1][0] < x_max:
        valid.append((x_max, valid[-1][1]))
    coords = [f"{_px(b, log_min, log_max):.1f},{_py(m):.1f}" for b, m in valid]
    return "M" + " L".join(coords)


# ---------------------------------------------------------------------------
# Combined HTML generation
# ---------------------------------------------------------------------------

def make_combined_html(all_snapshots, truth_xs, truth_ys, labels, interval, out_path: Path):
    """
    all_snapshots: list of lists, one per panel — each is [(snap_num, xs, ys), ...]
    """
    num_panels = len(all_snapshots)

    # Shared x range across all panels and ground truth
    all_x = [b for b in truth_xs if b > 0]
    for snaps in all_snapshots:
        for _, xs, _ in snaps:
            all_x.extend(x for x in xs if x > 0)
    log_min = math.log10(min(all_x))
    log_max = math.log10(max(all_x))

    # Build static SVG markup for each panel
    panel_svgs = []
    for i, label in enumerate(labels):
        panel_svgs.append(_panel_svg(i, truth_xs, truth_ys, log_min, log_max, label))

    # Precompute paths and MAEs for each panel at each snapshot index
    # All panels share the same animation index; clamp if a panel has fewer snapshots
    max_snaps = max(len(s) for s in all_snapshots)

    panel_paths = []   # panel_paths[panel][snap_index] = svg path string
    panel_maes  = []   # panel_maes[panel][snap_index] = mae float
    panel_nums  = []   # panel_nums[panel][snap_index] = snap_num int

    for snaps in all_snapshots:
        ppaths, pmaes, pnums = [], [], []
        for snap_num, xs, ys in snaps:
            ppaths.append(_shards_path(xs, ys, log_min, log_max))
            errors = [abs(_interpolate(xs, ys, tb) - tmr)
                      for tb, tmr in zip(truth_xs, truth_ys)
                      if _interpolate(xs, ys, tb) is not None]
            pmaes.append(round(sum(errors) / len(errors), 4) if errors else 0.0)
            pnums.append(snap_num)
        panel_paths.append(ppaths)
        panel_maes.append(pmaes)
        panel_nums.append(pnums)

    # Determine global snap_nums from the panel with most snapshots
    longest = max(range(num_panels), key=lambda i: len(panel_nums[i]))
    global_snap_nums = panel_nums[longest]

    # JS data
    panel_paths_js = json.dumps(panel_paths)
    panel_maes_js  = json.dumps(panel_maes)
    snap_nums_js   = json.dumps(global_snap_nums)
    last_snap      = global_snap_nums[-1]

    # Build panel SVG elements
    panel_svg_html = ""
    for i in range(num_panels):
        panel_svg_html += f"""
  <svg width="{PW}" height="{PH}" xmlns="http://www.w3.org/2000/svg" style="border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.12);">
    {panel_svgs[i]}
    <path id="shards-path-{i}" d="" fill="none" stroke="steelblue" stroke-width="2" stroke-linejoin="round"/>
  </svg>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SHARDS MRC Convergence — Experiment 2</title>
<style>
  body     {{ font-family: monospace; margin: 20px; background: #f5f5f5; }}
  h2       {{ margin-bottom: 10px; }}
  #panels  {{ display: flex; gap: 12px; flex-wrap: nowrap; overflow-x: auto; padding-bottom: 8px; }}
  #controls {{
    display: flex; align-items: center; gap: 20px;
    padding: 10px 18px; margin-bottom: 14px;
    background: white; border-radius: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,.12);
  }}
  button   {{ padding: 6px 20px; font-size: 14px; font-family: monospace;
              cursor: pointer; border: 1px solid #aaa; border-radius: 4px; background: #fff; }}
  button:hover {{ background: #eee; }}
  #slider  {{ width: 320px; cursor: pointer; }}
  .info    {{ font-size: 13px; line-height: 1.8; }}
  select   {{ font-family: monospace; font-size: 13px; padding: 3px 8px; }}
</style>
</head>
<body>
<h2>SHARDS MRC Convergence — Experiment 2</h2>
<div id="controls">
  <button id="btn">&#9654; Play</button>
  <input type="range" id="slider" min="0" max="{max_snaps-1}" value="0" step="1">
  <div class="info">
    <div id="snap-info">Snapshot 1 / {last_snap} &nbsp;|&nbsp; 1M accesses seen</div>
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

<div id="panels">{panel_svg_html}
</div>

<script>
const panelPaths = {panel_paths_js};
const panelMaes  = {panel_maes_js};
const snapNums   = {snap_nums_js};
const INTERVAL   = {interval};
const N          = {max_snaps};
const NUM_PANELS = {num_panels};

let cur     = 0;
let playing = false;
let timer   = null;

const btn      = document.getElementById('btn');
const slider   = document.getElementById('slider');
const snapInfo = document.getElementById('snap-info');
const speedSel = document.getElementById('speed');

const pathEls = Array.from({{length: NUM_PANELS}}, (_, i) => document.getElementById('shards-path-' + i));
const maeEls  = Array.from({{length: NUM_PANELS}}, (_, i) => document.getElementById('mae-' + i));

function fmtNum(n) {{
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(0) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(0) + 'K';
  return String(n);
}}

function render(i) {{
  for (let p = 0; p < NUM_PANELS; p++) {{
    const pi = Math.min(i, panelPaths[p].length - 1);
    pathEls[p].setAttribute('d', panelPaths[p][pi] || '');
    maeEls[p].textContent = 'MAE: ' + (panelMaes[p][pi] * 100).toFixed(2) + '%';
  }}
  const sn = snapNums[i];
  snapInfo.textContent =
    'Snapshot ' + sn + ' / ' + snapNums[snapNums.length-1] + '  |  ' + fmtNum(sn * INTERVAL) + ' accesses seen';
  slider.value = i;
}}

function step() {{
  if (cur >= N - 1) {{
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
    if (cur >= N - 1) cur = 0;
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
    ap.add_argument('--results-dir',    type=Path, required=True)
    ap.add_argument('--truth-run',      type=str,  default='exp2_trace')
    ap.add_argument('--shards-runs',    type=str,  nargs='+',
                    default=['exp2_shards_s1','exp2_shards_s01',
                             'exp2_shards_s001','exp2_shards_s0001'])
    ap.add_argument('--labels',         type=str,  nargs='+',
                    default=['s=1.0','s=0.1','s=0.01','s=0.001'])
    ap.add_argument('--avg-block-size', type=float, default=None)
    ap.add_argument('--filtered-csv',   type=Path,  default=None)
    ap.add_argument('--interval',       type=int,   default=1_000_000)
    ap.add_argument('--output-dir',     type=Path,  default=None)
    args = ap.parse_args()

    out_dir = args.output_dir or (args.results_dir / 'exp2_analysis')
    out_dir.mkdir(parents=True, exist_ok=True)

    # avg block size
    if args.avg_block_size:
        avg_bs = args.avg_block_size
        print(f"Using provided avg block size: {avg_bs:.1f} B")
    else:
        csv_path = (args.filtered_csv or
                    args.results_dir / args.truth_run / 'analysis' / 'filtered.csv')
        print(f"Computing avg block size from {csv_path} ...")
        avg_bs = avg_block_size_from_csv(csv_path)

    # ground truth
    truth_path = args.results_dir / args.truth_run / 'analysis' / 'mrc.txt'
    print(f"Reading ground truth from {truth_path} ...")
    truth_xs, truth_ys = read_ground_truth(truth_path)
    print(f"  {len(truth_xs)} ground truth points")

    # collect snapshots for each run
    labels = list(args.labels)
    while len(labels) < len(args.shards_runs):
        labels.append(args.shards_runs[len(labels)])

    all_snapshots = []
    for run_name, label in zip(args.shards_runs, labels):
        run_dir = args.results_dir / run_name
        print(f"\n[{label}]  {run_dir}")
        snaps = collect_snapshots(run_dir, avg_bs)
        print(f"  {len(snaps)} snapshots found")
        if not snaps:
            print("  WARNING: no snapshots — skipping")
            continue
        all_snapshots.append(snaps)

    out_file = out_dir / 'exp2_animation.html'
    print(f"\nGenerating combined HTML → {out_file}")
    make_combined_html(all_snapshots, truth_xs, truth_ys, labels, args.interval, out_file)
    print(f"\nDone. Output: {out_file}")


if __name__ == '__main__':
    main()
