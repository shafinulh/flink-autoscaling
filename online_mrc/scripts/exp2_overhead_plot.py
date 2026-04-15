"""
exp2_overhead_plot.py — wall time and peak RAM overhead comparison across SHARDS modes.

Reads overhead.txt files produced by run_overhead_exp.sh and generates a grouped
bar chart (wall time + peak RAM side by side) comparing plain vs shards at each
sampling rate.

Usage:
  python3 exp2_overhead_plot.py \\
    --results-dir benchmark/online-mrc/results \\
    --run-ids exp2_overhead_plain exp2_overhead_s1 exp2_overhead_s01 \\
              exp2_overhead_s001 exp2_overhead_s0001 \\
    --labels "plain" "s=1.0" "s=0.1" "s=0.01" "s=0.001" \\
    --output  benchmark/online-mrc/results/exp2_analysis/exp2_overhead.html
"""

import argparse
from pathlib import Path


COLORS = ['#90A4AE', '#2196F3', '#4CAF50', '#FF9800', '#E91E63']


def parse_overhead(path: Path):
    """Parse overhead.txt. Returns dict with wall_time_s and peak_rss_kb."""
    result = {}
    if not path.exists():
        return result
    with open(path) as f:
        for line in f:
            line = line.strip()
            if '=' in line:
                k, v = line.split('=', 1)
                result[k.strip()] = v.strip()
    return result


def bar_chart_svg(title, groups, values, unit, color_list, x_offset=0):
    """Return SVG markup for a single bar chart. groups = label list, values = float list."""
    W, H = 380, 320
    PAD_L, PAD_R, PAD_T, PAD_B = 70, 20, 50, 60
    cw = W - PAD_L - PAD_R
    ch = H - PAD_T - PAD_B

    y_max = max(values) * 1.15 if values else 1.0
    bar_w = cw / len(groups) * 0.6
    gap   = cw / len(groups)

    L = []
    L.append(f'<g transform="translate({x_offset},0)">')
    L.append(f'<rect width="{W}" height="{H}" fill="white" rx="6"/>')
    L.append(f'<text x="{W//2}" y="28" text-anchor="middle" font-size="13" '
             f'font-weight="bold" font-family="monospace">{title}</text>')
    L.append(f'<rect x="{PAD_L}" y="{PAD_T}" width="{cw}" height="{ch}" '
             f'fill="none" stroke="#aaa" stroke-width="1"/>')

    # Y grid + labels
    n_ticks = 5
    for i in range(n_ticks + 1):
        mv = y_max * i / n_ticks
        yg = PAD_T + (1.0 - mv / y_max) * ch
        L.append(f'<line x1="{PAD_L}" y1="{yg:.1f}" x2="{PAD_L+cw}" y2="{yg:.1f}" '
                 f'stroke="#eee" stroke-dasharray="3 3"/>')
        L.append(f'<text x="{PAD_L-5}" y="{yg+4:.1f}" text-anchor="end" '
                 f'font-size="10" fill="#666" font-family="monospace">{mv:.0f}</text>')

    # Y axis label
    rot_x, rot_y = 12, PAD_T + ch // 2
    L.append(f'<text x="{rot_x}" y="{rot_y}" text-anchor="middle" font-size="10" '
             f'fill="#333" font-family="monospace" '
             f'transform="rotate(-90,{rot_x},{rot_y})">{unit}</text>')

    # Bars
    for i, (group, value, color) in enumerate(zip(groups, values, color_list)):
        bx = PAD_L + gap * i + (gap - bar_w) / 2
        bh = (value / y_max) * ch
        by = PAD_T + ch - bh
        L.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
                 f'fill="{color}" rx="2"/>')
        # Value label on top of bar
        L.append(f'<text x="{bx + bar_w/2:.1f}" y="{by - 4:.1f}" text-anchor="middle" '
                 f'font-size="9" fill="#333" font-family="monospace">{value:.1f}</text>')
        # X label
        L.append(f'<text x="{bx + bar_w/2:.1f}" y="{PAD_T+ch+16}" text-anchor="middle" '
                 f'font-size="10" fill="#333" font-family="monospace">{group}</text>')

    L.append('</g>')
    return '\n'.join(L)


def to_html(run_ids, labels, overheads, output_path: Path):
    wall_times = []
    peak_rams  = []

    for rid in run_ids:
        d = overheads.get(rid, {})
        wt = float(d.get('wall_time_s', 0))
        rm = float(d.get('peak_rss_kb', 0)) / 1024  # KB → MB
        wall_times.append(wt)
        peak_rams.append(rm)

    # Two charts side by side
    chart_w = 400
    total_w = chart_w * 2 + 20
    total_h = 360

    wt_svg  = bar_chart_svg('Wall Time', labels, wall_times, 'seconds', COLORS, x_offset=0)
    ram_svg = bar_chart_svg('Peak RAM', labels, peak_rams,  'MB',      COLORS, x_offset=chart_w + 20)

    # Plain baseline delta annotations
    wt_plain  = wall_times[0] if wall_times else 1.0
    ram_plain = peak_rams[0]  if peak_rams  else 1.0

    rows = []
    for label, wt, rm in zip(labels, wall_times, peak_rams):
        wt_delta  = f'+{wt - wt_plain:.1f}s ({(wt/wt_plain - 1)*100:.1f}%)'   if wt_plain > 0 else '—'
        ram_delta = f'+{rm - ram_plain:.0f}MB ({(rm/ram_plain - 1)*100:.0f}%)' if ram_plain > 0 else '—'
        if label == labels[0]:
            wt_delta = ram_delta = '(baseline)'
        rows.append(f'<tr><td>{label}</td><td>{wt:.1f}s</td><td>{wt_delta}</td>'
                    f'<td>{rm:.0f}MB</td><td>{ram_delta}</td></tr>')

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Exp2 Overhead</title>
<style>
body  {{font-family:monospace;margin:20px;background:#f5f5f5;}}
svg   {{background:white;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.12);display:block;}}
table {{border-collapse:collapse;margin-top:20px;background:white;
       border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.12);}}
th,td {{padding:8px 16px;border-bottom:1px solid #eee;text-align:left;}}
th    {{background:#f5f5f5;}}
</style>
</head>
<body>
<h2>Exp2: SHARDS Overhead vs Plain (no instrumentation)</h2>
<svg width="{total_w}" height="{total_h}" xmlns="http://www.w3.org/2000/svg"
     font-family="monospace" font-size="12">
  <rect width="{total_w}" height="{total_h}" fill="white" rx="6"/>
  {wt_svg}
  {ram_svg}
</svg>
<table>
  <tr><th>Mode</th><th>Wall Time</th><th>vs Plain</th><th>Peak RAM</th><th>vs Plain</th></tr>
  {''.join(rows)}
</table>
</body></html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    print(f"Saved: {output_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results-dir', type=Path, required=True)
    ap.add_argument('--run-ids',     type=str, nargs='+',
                    default=['exp2_overhead_plain','exp2_overhead_s1',
                             'exp2_overhead_s01','exp2_overhead_s001',
                             'exp2_overhead_s0001'])
    ap.add_argument('--labels',      type=str, nargs='+',
                    default=['plain','s=1.0','s=0.1','s=0.01','s=0.001'])
    ap.add_argument('--output',      type=Path, required=True)
    args = ap.parse_args()

    overheads = {}
    for rid in args.run_ids:
        p = args.results_dir / rid / 'overhead.txt'
        overheads[rid] = parse_overhead(p)
        print(f"{rid}: {overheads[rid]}")

    to_html(args.run_ids, args.labels, overheads, args.output)


if __name__ == '__main__':
    main()
