"""
exp2_animation.py — Experiment 2 SHARDS MRC convergence animation.

Generates a single HTML file with 4 side-by-side panels (one per sampling rate),
all driven by shared playback controls. Each panel shows:
  - SHARDS snapshot (blue)         : online approximation at each checkpoint
  - GT snapshot (orange dashed)    : exact LRU MRC using same # of accesses
  - Final GT reference (orange dots): full ground truth over all accesses

At s=1.0, the SHARDS and GT snapshot lines should track each other frame-by-frame.

Usage:
  python3 exp2_animation.py \\
    --results-dir      benchmark/online-mrc/results \\
    --truth-run        exp2_trace \\
    --shards-runs      exp2_shards_s1 exp2_shards_s01 exp2_shards_s001 exp2_shards_s0001 \\
    --labels           "s=1.0" "s=0.1" "s=0.01" "s=0.001" \\
    --filtered-csv     /tom/distr_project/exp2_trace/analysis/filtered.csv \\
    --gt-snapshots-dir benchmark/online-mrc/results/exp2_trace/analysis/gt_snapshots \\
    --interval         1000000 \\
    --output-dir       benchmark/online-mrc/results/exp2_analysis
"""

import argparse
import json
import math
from pathlib import Path


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_shards_txt(path: Path, avg_bs: float, sampling_ratio: float = 1.0):
    """Read a SHARDS .txt snapshot. Returns (bytes_list, miss_ratio_list).

    cache_units (blocks) are converted to bytes via:
        cache_bytes = cache_units * avg_bs / sampling_ratio
    The 1/s rescale corrects for the fact that the order-statistics tree measures
    stack distances in sampled-block units; dividing by s maps back to actual blocks.
    """
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
            xs.append(cache_units * avg_bs / sampling_ratio)
            ys.append(miss_ratio)
    return xs, ys


def read_ground_truth(path: Path):
    """Read mrc.txt or gt_snapshot_N.txt. Returns (bytes_list, miss_ratio_0to1_list)."""
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


def collect_snapshots(run_dir: Path, avg_bs: float, sampling_ratio: float = 1.0):
    """Collect SHARDS snapshot .txt files. Returns [(snap_num, xs, ys), ...]."""
    snaps = []
    for p in run_dir.iterdir():
        name = p.name
        if name.startswith('online_mrc.bin.') and name.endswith('.txt'):
            stem = name[len('online_mrc.bin.'):-len('.txt')]
            try:
                n = int(stem)
            except ValueError:
                continue
            xs, ys = read_shards_txt(p, avg_bs, sampling_ratio)
            snaps.append((n, xs, ys))
    snaps.sort(key=lambda s: s[0])
    return snaps


def collect_gt_snapshots(gt_dir: Path):
    """Collect GT snapshot files. Returns [(snap_num, xs, ys), ...]."""
    snaps = []
    for p in gt_dir.iterdir():
        name = p.name
        if name.startswith('gt_snapshot_') and name.endswith('.txt'):
            stem = name[len('gt_snapshot_'):-len('.txt')]
            try:
                n = int(stem)
            except ValueError:
                continue
            xs, ys = read_ground_truth(p)
            snaps.append((n, xs, ys))
    snaps.sort(key=lambda s: s[0])
    return snaps


# ---------------------------------------------------------------------------
# Panel drawing helpers
# ---------------------------------------------------------------------------

PW, PH = 400, 400
PP_L, PP_R, PP_T, PP_B = 60, 10, 35, 60
pw = PW - PP_L - PP_R   # 330
ph = PH - PP_T - PP_B   # 305


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


def _panel_svg(panel_idx, truth_xs, truth_ys, log_min, log_max, label, has_gt_snaps):
    """Return SVG markup for one panel's static elements."""
    L = []

    L.append(f'<rect width="{PW}" height="{PH}" fill="white"/>')
    L.append(
        f'<text x="{PW//2}" y="28" text-anchor="middle" '
        f'font-size="13" font-weight="bold" font-family="monospace">{label}</text>'
    )
    L.append(
        f'<rect x="{PP_L}" y="{PP_T}" width="{pw}" height="{ph}" '
        f'fill="none" stroke="#aaa" stroke-width="1"/>'
    )

    # X grid + labels (powers of 2)
    p2_start = math.ceil(math.log2(10 ** log_min))
    p2_end   = math.floor(math.log2(10 ** log_max))
    for p in range(p2_start, p2_end + 1):
        b  = 2 ** p
        xg = _px(b, log_min, log_max)
        L.append(f'<line x1="{xg:.1f}" y1="{PP_T}" x2="{xg:.1f}" y2="{PP_T+ph}" '
                 f'stroke="#eee" stroke-dasharray="3 3"/>')
        if p % 2 == 0:   # label every 4× step to avoid overlap
            L.append(
                f'<text x="{xg:.1f}" y="{PP_T+ph+15}" text-anchor="middle" '
                f'font-size="9" fill="#666" font-family="monospace">{_fmt_bytes(b)}</text>'
            )

    # Y grid + labels (leftmost panel only)
    for tick in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]:
        yg = _py(tick)
        L.append(f'<line x1="{PP_L}" y1="{yg:.1f}" x2="{PP_L+pw}" y2="{yg:.1f}" '
                 f'stroke="#eee" stroke-dasharray="3 3"/>')
        L.append(
                f'<text x="{PP_L-5}" y="{yg+4:.1f}" text-anchor="end" '
                f'font-size="10" fill="#666" font-family="monospace">{tick:.1f}</text>'
            )

    # Axis labels
    L.append(
        f'<text x="{PP_L+pw//2}" y="{PP_T+ph+40}" text-anchor="middle" '
        f'font-size="12" fill="#333" font-family="monospace">Cache size (log scale)</text>'
    )
    cx, cy = 22, PP_T + ph // 2
    L.append(
        f'<text x="{cx}" y="{cy}" text-anchor="middle" font-size="12" fill="#333" '
        f'font-family="monospace" transform="rotate(-90,{cx},{cy})">Miss ratio</text>'
    )

    # Final ground truth dots (reference — always visible; skip points left of x-axis minimum)
    valid = [(b, m) for b, m in zip(truth_xs, truth_ys) if 10**log_min <= b <= 10**log_max * 1.001]
    for b, m in valid:
        L.append(
            f'<circle cx="{_px(b,log_min,log_max):.1f}" cy="{_py(m):.1f}" '
            f'r="3" fill="#f90" stroke="white" stroke-width="1"/>'
        )

    # Legend
    lx = PP_L + 6
    ly = PP_T + ph + 20
    L.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+18}" y2="{ly}" '
             f'stroke="steelblue" stroke-width="2"/>')
    L.append(f'<text x="{lx+22}" y="{ly+4}" font-size="9" fill="#333" '
             f'font-family="monospace">SHARDS</text>')
    if has_gt_snaps:
        L.append(f'<line x1="{lx+100}" y1="{ly}" x2="{lx+118}" y2="{ly}" '
                 f'stroke="#f90" stroke-width="2" stroke-dasharray="5 3"/>')
        L.append(f'<text x="{lx+122}" y="{ly+4}" font-size="9" fill="#333" '
                 f'font-family="monospace">GT snapshot</text>')
    L.append(f'<circle cx="{lx+220}" cy="{ly}" r="3" fill="#f90"/>')
    L.append(f'<text x="{lx+228}" y="{ly+4}" font-size="9" fill="#333" '
             f'font-family="monospace">GT final</text>')


    return "\n    ".join(L)


def _curve_path(xs, ys, log_min, log_max):
    """SVG path: straight lines covering only the data range."""
    valid = [(b, m) for b, m in zip(xs, ys) if 10**log_min <= b <= 10**log_max * 1.001 and 0.0 <= m <= 1.0]
    if not valid:
        return ""
    coords = [f"{_px(b, log_min, log_max):.1f},{_py(m):.1f}" for b, m in valid]
    return "M" + " L".join(coords)


# ---------------------------------------------------------------------------
# Combined HTML generation
# ---------------------------------------------------------------------------

def make_combined_html(all_snapshots, all_gt_snapshots,
                       truth_xs, truth_ys, labels, interval, out_path: Path):
    num_panels = len(all_snapshots)
    has_gt = len(all_gt_snapshots) > 0

    # Shared x range
    all_x = [b for b in truth_xs if b > 0]
    for snaps in all_snapshots:
        for _, xs, _ in snaps:
            all_x.extend(x for x in xs if x > 0)
    if has_gt:
        for _, xs, _ in all_gt_snapshots:
            all_x.extend(x for x in xs if x > 0)
    X_MIN_BYTES = 1 * 1024 * 1024   # 1MB floor
    X_MAX_BYTES = 16 * 1024 ** 3    # 16GiB ceiling
    log_min = math.log10(max(min(all_x), X_MIN_BYTES))
    log_max = math.log10(min(max(all_x), X_MAX_BYTES))

    # Static panel SVGs
    panel_svgs = [
        _panel_svg(i, truth_xs, truth_ys, log_min, log_max, labels[i], has_gt)
        for i in range(num_panels)
    ]

    # Precompute SHARDS paths + MAEs
    max_snaps = max(len(s) for s in all_snapshots)
    panel_paths, panel_maes, panel_nums = [], [], []

    for snaps in all_snapshots:
        ppaths, pmaes, pnums = [], [], []
        for snap_num, xs, ys in snaps:
            ppaths.append(_curve_path(xs, ys, log_min, log_max))
            errors = [abs(_interpolate(xs, ys, tb) - tmr)
                      for tb, tmr in zip(truth_xs, truth_ys)
                      if _interpolate(xs, ys, tb) is not None]
            pmaes.append(round(sum(errors) / len(errors), 4) if errors else 0.0)
            pnums.append(snap_num)
        panel_paths.append(ppaths)
        panel_maes.append(pmaes)
        panel_nums.append(pnums)

    # Precompute GT snapshot paths + MAEs (single shared series, all panels use same GT)
    gt_paths, gt_maes, gt_nums = [], [], []
    if has_gt:
        for snap_num, xs, ys in all_gt_snapshots:
            gt_paths.append(_curve_path(xs, ys, log_min, log_max))
            errors = [abs(_interpolate(xs, ys, tb) - tmr)
                      for tb, tmr in zip(truth_xs, truth_ys)
                      if _interpolate(xs, ys, tb) is not None]
            gt_maes.append(round(sum(errors) / len(errors), 4) if errors else 0.0)
            gt_nums.append(snap_num)

    longest = max(range(num_panels), key=lambda i: len(panel_nums[i]))
    global_snap_nums = panel_nums[longest]
    last_snap = global_snap_nums[-1]

    # JS data
    panel_paths_js = json.dumps(panel_paths)
    panel_maes_js  = json.dumps(panel_maes)
    snap_nums_js   = json.dumps(global_snap_nums)
    gt_paths_js    = json.dumps(gt_paths)
    gt_maes_js     = json.dumps(gt_maes)

    # Build panel SVG elements
    panel_svg_html = ""
    for i in range(num_panels):
        gt_path_el = (f'<path id="gt-path-{i}" d="" fill="none" stroke="#f90" '
                      f'stroke-width="2" stroke-dasharray="6 3" stroke-linejoin="round"/>') if has_gt else ''
        panel_svg_html += f"""
  <svg width="{PW}" height="{PH}" xmlns="http://www.w3.org/2000/svg" style="border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.12);">
    {panel_svgs[i]}
    {gt_path_el}
    <path id="shards-path-{i}" d="" fill="none" stroke="steelblue" stroke-width="2" stroke-linejoin="round"/>
  </svg>"""

    has_gt_js  = json.dumps(has_gt)
    num_panels_js = num_panels

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SHARDS MRC Convergence — Experiment 2</title>
<style>
  body     {{ font-family: monospace; margin: 20px; background: #f5f5f5; }}
  h2       {{ margin-bottom: 10px; }}
  #panels  {{ display: grid; grid-template-columns: repeat(3, {PW}px); gap: 12px; padding-bottom: 8px; }}
  #controls {{
    display: flex; align-items: center; gap: 20px;
    padding: 10px 18px; margin-bottom: 14px;
    background: white; border-radius: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,.12);
  }}
  button  {{ padding: 6px 20px; font-size: 14px; font-family: monospace;
             cursor: pointer; border: 1px solid #aaa; border-radius: 4px; background: #fff; }}
  button:hover {{ background: #eee; }}
  #slider {{ width: 320px; cursor: pointer; }}
  .info   {{ font-size: 13px; line-height: 1.8; }}
  select  {{ font-family: monospace; font-size: 13px; padding: 3px 8px; }}
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
const gtPaths    = {gt_paths_js};
const gtMaes     = {gt_maes_js};
const hasGT      = {has_gt_js};
const INTERVAL   = {interval};
const N          = {max_snaps};
const NUM_PANELS = {num_panels_js};

let cur     = 0;
let playing = false;
let timer   = null;

const btn      = document.getElementById('btn');
const slider   = document.getElementById('slider');
const snapInfo = document.getElementById('snap-info');
const speedSel = document.getElementById('speed');

const shardsEls  = Array.from({{length: NUM_PANELS}}, (_, i) => document.getElementById('shards-path-' + i));
const gtEls      = hasGT ? Array.from({{length: NUM_PANELS}}, (_, i) => document.getElementById('gt-path-' + i)) : [];

function fmtNum(n) {{
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(0) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(0) + 'K';
  return String(n);
}}

function render(i) {{
  const gtI = hasGT ? Math.min(i, gtPaths.length - 1) : 0;
  for (let p = 0; p < NUM_PANELS; p++) {{
    const pi = Math.min(i, panelPaths[p].length - 1);
    shardsEls[p].setAttribute('d', panelPaths[p][pi] || '');
    if (hasGT) {{
      gtEls[p].setAttribute('d', gtPaths[gtI] || '');
    }}
  }}
  const sn = snapNums[i];
  snapInfo.textContent =
    'Snapshot ' + sn + ' / ' + snapNums[snapNums.length-1] +
    '  |  ' + fmtNum(sn * INTERVAL) + ' accesses seen';
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
    ap.add_argument('--results-dir',      type=Path, required=True)
    ap.add_argument('--truth-run',        type=str,  default='exp2_trace')
    ap.add_argument('--truth-mrc',        type=Path, default=None,
                    help='Direct path to mrc.txt (overrides --truth-run)')
    ap.add_argument('--shards-runs',      type=str,  nargs='+',
                    default=['exp2_shards_s1','exp2_shards_s01',
                             'exp2_shards_s001','exp2_shards_s0001'])
    ap.add_argument('--labels',           type=str,  nargs='+',
                    default=['s=1.0','s=0.1','s=0.01','s=0.001'])
    ap.add_argument('--avg-block-size',   type=float, default=None)
    ap.add_argument('--sampling-ratios',  type=float, nargs='+',
                    default=[1.0, 0.1, 0.01, 0.001],
                    help='Sampling ratio per --shards-runs entry (same order). '
                         'Used to rescale x-axis by 1/s.')
    ap.add_argument('--filtered-csv',     type=Path,  default=None)
    ap.add_argument('--gt-snapshots-dir', type=Path,  default=None)
    ap.add_argument('--interval',         type=int,   default=1_000_000)
    ap.add_argument('--output-dir',       type=Path,  default=None)
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

    # ground truth final MRC
    if args.truth_mrc:
        truth_path = args.truth_mrc
    else:
        truth_path = args.results_dir / args.truth_run / 'analysis' / 'mrc.txt'
    print(f"Reading ground truth from {truth_path} ...")
    truth_xs, truth_ys = read_ground_truth(truth_path)
    print(f"  {len(truth_xs)} ground truth points")

    # GT snapshots (optional)
    all_gt_snapshots = []
    if args.gt_snapshots_dir and args.gt_snapshots_dir.exists():
        print(f"Loading GT snapshots from {args.gt_snapshots_dir} ...")
        all_gt_snapshots = collect_gt_snapshots(args.gt_snapshots_dir)
        print(f"  {len(all_gt_snapshots)} GT snapshots loaded")
    else:
        print("No GT snapshots dir provided — animation will show SHARDS only.")

    # SHARDS snapshots per run
    labels = list(args.labels)
    while len(labels) < len(args.shards_runs):
        labels.append(args.shards_runs[len(labels)])

    sampling_ratios = list(args.sampling_ratios)
    while len(sampling_ratios) < len(args.shards_runs):
        sampling_ratios.append(1.0)

    all_snapshots = []
    for run_name, label, s in zip(args.shards_runs, labels, sampling_ratios):
        run_dir = args.results_dir / run_name
        print(f"\n[{label}]  {run_dir}  (s={s})")
        snaps = collect_snapshots(run_dir, avg_bs, s)
        print(f"  {len(snaps)} snapshots found")
        if not snaps:
            print("  WARNING: no snapshots — skipping")
            continue
        all_snapshots.append(snaps)

    out_file = out_dir / 'exp2_animation.html'
    print(f"\nGenerating combined HTML → {out_file}")
    make_combined_html(all_snapshots, all_gt_snapshots,
                       truth_xs, truth_ys, labels, args.interval, out_file)
    print(f"\nDone. Output: {out_file}")


if __name__ == '__main__':
    main()
