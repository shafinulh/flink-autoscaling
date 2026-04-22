"""
decode_mrc_bin.py — Convert online_mrc.bin (and snapshot) files to .txt format.

Binary format (from shards_mrc.cc DumpMRC):
  header:  num_bins (uint64), bin_size (uint64)
  entries: index (uint64), miss_rate (float64)  × num_bins

Output .txt format (read by read_shards() in plotting scripts):
  cache_size_bytes,miss_ratio
  <index * bin_size>,<miss_rate>
  ...

Usage:
  # Decode all .bin files in a directory:
  python3 decode_mrc_bin.py --dir results/exp3_uniform_shards

  # Decode specific file(s):
  python3 decode_mrc_bin.py results/exp3_uniform_shards/online_mrc.bin

  # Decode all 4 exp3 directories at once:
  python3 decode_mrc_bin.py \\
    --dir results/exp3_uniform_shards \\
    --dir results/exp3_forward_switch \\
    --dir results/exp3_reverse_switch
"""

import argparse
import struct
import sys
from pathlib import Path


def decode_bin(bin_path: Path) -> list[tuple[int, float]]:
    with open(bin_path, 'rb') as f:
        hdr = f.read(16)
        if len(hdr) < 16:
            raise ValueError(f"File too short for header: {bin_path}")
        num_bins, bin_size = struct.unpack('<QQ', hdr)
        entries = []
        for i in range(num_bins):
            chunk = f.read(16)  # uint64 + double = 8 + 8
            if len(chunk) < 16:
                break
            idx, miss_rate = struct.unpack('<Qd', chunk)
            entries.append((idx * bin_size, miss_rate))
    return entries


def write_txt(entries: list[tuple[int, float]], txt_path: Path):
    with open(txt_path, 'w') as f:
        f.write('cache_size_bytes,miss_ratio\n')
        for cache_units, miss_rate in entries:
            f.write(f'{cache_units},{miss_rate:.6f}\n')


def process_file(bin_path: Path, overwrite: bool = False):
    txt_path = bin_path.with_suffix(bin_path.suffix + '.txt')
    if txt_path.exists() and not overwrite:
        print(f'  skip (exists): {txt_path.name}')
        return
    try:
        entries = decode_bin(bin_path)
        write_txt(entries, txt_path)
        print(f'  decoded: {txt_path.name}  ({len(entries)} bins)')
    except Exception as e:
        print(f'  ERROR {bin_path.name}: {e}', file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('files', nargs='*', type=Path,
                    help='Specific .bin files to decode')
    ap.add_argument('--dir', dest='dirs', action='append', type=Path, default=[],
                    metavar='DIR',
                    help='Directory; decode all .bin files inside (repeatable)')
    ap.add_argument('--overwrite', action='store_true',
                    help='Overwrite existing .txt files')
    args = ap.parse_args()

    targets: list[Path] = list(args.files)
    for d in args.dirs:
        targets += sorted(p for p in d.iterdir()
                          if p.name.startswith('online_mrc.bin') and not p.suffix == '.txt')

    if not targets:
        ap.print_help()
        sys.exit(1)

    for p in targets:
        process_file(p, overwrite=args.overwrite)


if __name__ == '__main__':
    main()
