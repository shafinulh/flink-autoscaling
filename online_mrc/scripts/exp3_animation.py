"""
exp3_animation.py — Experiment 3 distribution-shift animation.

Single-panel HTML animation showing SHARDS MRC snapshots across a workload
that switches from uniform key distribution to Zipfian at the 50% mark.
A vertical dashed line marks the phase-switch snapshot so the viewer can
see how the MRC drifts as SHARDS history is polluted by the uniform phase.

Usage:
  python3 exp3_animation.py \\
    --results-dir  benchmark/online-mrc/results \\
    --shards-run   exp3_phase_switch \\
    --avg-block-size 4080 \\
    --interval     1000000 \\
    --output       benchmark/online-mrc/results/exp3_analysis/exp3_animation.html
"""

import argparse
import json
import math
from pathlib import Path


# ---------------------------------------------------------------------------
# Readers (same as exp2_animation.py)
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
    """Read mrc.txt. Returns (bytes_list, miss_ratio_0to1_list)."""
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


def collect_snapshots(run_dir: Path, avg_bs: float):
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
            xs, ys = read_shards_txt(p, avg_bs)
            snaps.append((n, xs, ys))
    snaps.sort(key=lambda s: s[0])
    return snaps


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

PW, PH = 700, 480
PP_L, PP_R, PP_T, PP_B = 65, 20, 40, 65
pw = PW - PP_L - PP_R
ph = PH - PP_T - PP_B


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


def _curve_path(xs, ys, log_min, log_max):
    valid = [(b, m) for b, m in zip(xs, ys)
             if 10**log_min <= b <= 10**log_max * 1.001 and 0.0 <= m <= 1.0]
    if not valid:
        return ""
    coords = [f"{_px(b, log_min, log_max):.1f},{_py(m):.1f}" for b, m in valid]
    return "M" + " L".join(coords)


def _static_svg(truth_xs, truth_ys, log_min, log_max):
    L = []

    L.append(f'<rect width="{PW}" height="{PH}" fill="white"/>')
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
        if p % 2 == 0:
            L.append(
                f'<text x="{xg:.1f}" y="{PP_T+ph+16}" text-anchor="middle" '
                f'font-size="10" fill="#666" font-family="monospace">{_fmt_bytes(b)}</text>'
            )

    # Y grid + labels
    for tick in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]:
        yg = _py(tick)
        L.append(f'<line x1="{PP_L}" y1="{yg:.1f}" x2="{PP_L+pw}" y2="{yg:.1f}" '
                 f'stroke="#eee" stroke-dasharray="3 3"/>')
        L.append(
            f'<text x="{PP_L-6}" y="{yg+4:.1f}" text-anchor="end" '
            f'font-size="11" fill="#666" font-family="monospace">{tick:.1f}</text>'
        )

    # Axis titles
    L.append(
        f'<text x="{PP_L+pw//2}" y="{PP_T+ph+45}" text-anchor="middle" '
        f'font-size="13" fill="#333" font-family="monospace">Cache size (log scale)</text>'
    )
    cx, cy = 18, PP_T + ph // 2
    L.append(
        f'<text x="{cx}" y="{cy}" text-anchor="middle" font-size="13" fill="#333" '
        f'font-family="monospace" transform="rotate(-90,{cx},{cy})">Miss ratio</text>'
    )

    # Ground truth dots (if provided)
    if truth_xs:
        valid = [(b, m) for b, m in zip(truth_xs, truth_ys)
                 if 10**log_min <= b <= 10**log_max * 1.001]
        for b, m in valid:
            L.append(
                f'<circle cx="{_px(b,log_min,log_max):.1f}" cy="{_py(m):.1f}" '
                f'r="3.5" fill="#f90" stroke="white" stroke-width="1"/>'
            )

    # Legend
    lx, ly = PP_L + 8, PP_T + ph + 50
    L.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+22}" y2="{ly}" '
             f'stroke="steelblue" stroke-width="2.5"/>')
    L.append(f'<text x="{lx+26}" y="{ly+4}" font-size="10" fill="#333" '
             f'font-family="monospace">SHARDS (s=1.0)</text>')
    if truth_xs:
        L.append(f'<circle cx="{lx+180}" cy="{ly}" r="4" fill="#f90"/>')
        L.append(f'<text x="{lx+188}" y="{ly+4}" font-size="10" fill="#333" '
                 f'font-family="monospace">Ground truth</text>')
    L.append(f'<line x1="{lx+340}" y1="{ly}" x2="{lx+362}" y2="{ly}" '
             f'stroke="#c44" stroke-width="1.5" stroke-dasharray="6 4"/>')
    L.append(f'<text x="{lx+366}" y="{ly+4}" font-size="10" fill="#333" '
             f'font-family="monospace">Phase switch</text>')

    return "\n    ".join(L)


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def make_html(snapshots, truth_xs, truth_ys, interval, phase_switch_accesses, out_path: Path):
    all_x = [x for _, xs, _ in snapshots for x in xs if x > 0]
    if truth_xs:
        all_x.extend(b for b in truth_xs if b > 0)
    X_MIN_BYTES = 1 * 1024 * 1024
    X_MAX_BYTES = 16 * 1024 ** 3
    log_min = math.log10(max(min(all_x), X_MIN_BYTES))
    log_max = math.log10(min(max(all_x), X_MAX_BYTES))

    static_svg = _static_svg(truth_xs, truth_ys, log_min, log_max)

    paths = []
    snap_nums = []
    for snap_num, xs, ys in snapshots:
        paths.append(_curve_path(xs, ys, log_min, log_max))
        snap_nums.append(snap_num)

    # Phase switch vertical line x-position (based on accesses seen)
    switch_snap = phase_switch_accesses / interval  # fractional snap number
    # Find the snap index nearest to the switch point
    switch_idx = min(range(len(snap_nums)),
                     key=lambda i: abs(snap_nums[i] - switch_snap))
    switch_snap_num = snap_nums[switch_idx]
    switch_x = PP_L + (switch_idx / max(len(snap_nums) - 1, 1)) * pw
    # Compute actual pixel x based on time, not cache size — use a JS variable instead
    # The phase switch line is at a fixed cache-axis position but we want it at a
    # fixed *time* position. We'll draw it as a time marker via JS on a separate overlay.

    paths_js    = json.dumps(paths)
    snap_nums_js = json.dumps(snap_nums)
    has_gt_js   = json.dumps(bool(truth_xs))
    switch_snap_js = switch_snap_num  # snap number closest to phase switch

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SHARDS MRC — Experiment 3: Distribution Shift</title>
<style>
  body     {{ font-family: monospace; margin: 20px; background: #f5f5f5; }}
  h2       {{ margin-bottom: 4px; }}
  p.desc   {{ font-size: 13px; color: #555; margin: 0 0 12px 0; }}
  #controls {{
    display: flex; align-items: center; gap: 20px;
    padding: 10px 18px; margin-bottom: 14px;
    background: white; border-radius: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,.12);
    width: fit-content;
  }}
  button {{ padding: 6px 20px; font-size: 14px; font-family: monospace;
            cursor: pointer; border: 1px solid #aaa; border-radius: 4px; background: #fff; }}
  button:hover {{ background: #eee; }}
  #slider  {{ width: 360px; cursor: pointer; }}
  .info    {{ font-size: 13px; line-height: 1.8; min-width: 260px; }}
  #phase-label {{ font-weight: bold; }}
  select {{ font-family: monospace; font-size: 13px; padding: 3px 8px; }}
</style>
</head>
<body>
<h2>SHARDS MRC Convergence — Experiment 3: Distribution Shift</h2>
<p class="desc">First 50% of ops: <b>Uniform</b> &nbsp;→&nbsp; Last 50% of ops: <b>Zipfian (α=0.6)</b> &nbsp;|&nbsp; s=1.0 (full sampling)</p>
<div id="controls">
  <button id="btn">&#9654; Play</button>
  <input type="range" id="slider" min="0" max="{len(snapshots)-1}" value="0" step="1">
  <div class="info">
    <div id="snap-info">Snapshot 1 / {snap_nums[-1]}</div>
    <div id="phase-label" style="color: steelblue;">Phase: Uniform</div>
  </div>
  <label>Speed:
    <select id="speed">
      <option value="1000">0.5&#215;</option>
      <option value="600" selected>1&#215;</option>
      <option value="300">2&#215;</option>
      <option value="120">5&#215;</option>
    </select>
  </label>
</div>

<svg width="{PW}" height="{PH}" xmlns="http://www.w3.org/2000/svg"
     style="border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.12);display:block;">
  {static_svg}
  <path id="shards-path" d="" fill="none" stroke="steelblue" stroke-width="2.5" stroke-linejoin="round"/>
  <line id="switch-line" x1="0" y1="{PP_T}" x2="0" y2="{PP_T+ph}"
        stroke="#c44" stroke-width="1.5" stroke-dasharray="6 4" opacity="0"/>
  <text id="switch-label" x="0" y="{PP_T-6}" text-anchor="middle"
        font-size="10" fill="#c44" font-family="monospace" opacity="0">switch</text>
</svg>

<script>
const paths    = {paths_js};
const snapNums = {snap_nums_js};
const INTERVAL = {interval};
const SWITCH_SNAP = {switch_snap_js};
const N = {len(snapshots)};
const PP_L = {PP_L}, PP_R = {PP_R}, PP_T = {PP_T}, PP_B = {PP_B};
const PW = {PW}, PH = {PH};
const pw = PW - PP_L - PP_R;

let cur = 0, playing = false, timer = null;

const btn       = document.getElementById('btn');
const slider    = document.getElementById('slider');
const snapInfo  = document.getElementById('snap-info');
const phaseLabel = document.getElementById('phase-label');
const shardsEl  = document.getElementById('shards-path');
const switchLine = document.getElementById('switch-line');
const switchLabel = document.getElementById('switch-label');
const speedSel  = document.getElementById('speed');

function fmtNum(n) {{
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(0) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(0) + 'K';
  return String(n);
}}

function render(i) {{
  shardsEl.setAttribute('d', paths[i] || '');
  const sn = snapNums[i];
  const accesses = sn * INTERVAL;
  snapInfo.textContent = 'Snapshot ' + sn + ' / ' + snapNums[N-1] +
    '  |  ' + fmtNum(accesses) + ' accesses seen';

  const inZipf = sn > SWITCH_SNAP;
  phaseLabel.textContent = inZipf ? 'Phase: Zipfian (α=0.6)' : 'Phase: Uniform';
  phaseLabel.style.color = inZipf ? '#c44' : 'steelblue';

  // Show phase switch line once we're past it
  if (sn >= SWITCH_SNAP) {{
    // x position: fraction of slider progress at the switch snapshot
    const switchIdx = snapNums.indexOf(SWITCH_SNAP);
    const switchFrac = switchIdx >= 0 ? switchIdx / (N - 1) : 0.5;
    const sx = PP_L + switchFrac * pw;
    switchLine.setAttribute('x1', sx);
    switchLine.setAttribute('x2', sx);
    switchLabel.setAttribute('x', sx);
    switchLine.setAttribute('opacity', '1');
    switchLabel.setAttribute('opacity', '1');
  }} else {{
    switchLine.setAttribute('opacity', '0');
    switchLabel.setAttribute('opacity', '0');
  }}

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
    ap.add_argument('--results-dir',          type=Path, required=True)
    ap.add_argument('--shards-run',           type=str,  default='exp3_phase_switch')
    ap.add_argument('--truth-mrc',            type=Path, default=None,
                    help='Optional path to ground truth mrc.txt')
    ap.add_argument('--avg-block-size',       type=float, default=4080.0,
                    help='Average data block size in bytes (default: 4080, from exp2)')
    ap.add_argument('--interval',             type=int,  default=1_000_000,
                    help='SHARDS snapshot interval in accesses (default: 1M)')
    ap.add_argument('--phase-switch-accesses', type=int, default=10_000_000,
                    help='Total accesses at which the distribution switches (default: 10M = half of 20M ops)')
    ap.add_argument('--output',               type=Path, default=None)
    args = ap.parse_args()

    run_dir = args.results_dir / args.shards_run
    out_path = args.output or (args.results_dir / 'exp3_analysis' / 'exp3_animation.html')
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Loading SHARDS snapshots from {run_dir} ...")
    snaps = collect_snapshots(run_dir, args.avg_block_size)
    print(f"  {len(snaps)} snapshots found")
    if not snaps:
        print("ERROR: no snapshots found. Check --shards-run and --results-dir.")
        return

    truth_xs, truth_ys = [], []
    if args.truth_mrc:
        print(f"Loading ground truth from {args.truth_mrc} ...")
        truth_xs, truth_ys = read_ground_truth(args.truth_mrc)
        print(f"  {len(truth_xs)} GT points")
    else:
        print("No --truth-mrc provided — plot will show SHARDS only.")

    print(f"Phase switch at {args.phase_switch_accesses:,} accesses "
          f"(snapshot ~{args.phase_switch_accesses / args.interval:.0f})")
    print(f"Generating HTML → {out_path}")
    make_html(snaps, truth_xs, truth_ys, args.interval,
              args.phase_switch_accesses, out_path)
    print("Done.")


if __name__ == '__main__':
    main()
