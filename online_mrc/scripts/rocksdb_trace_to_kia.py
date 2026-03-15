"""
Convert a RocksDB block cache trace (human-readable CSV) to the Kia binary
format expected by online_mrc's generate_mrc_exe.

RocksDB CSV columns (from block_cache_tracer.h):
  0: access_timestamp (microseconds, u64)
  1: block_id         (unique block identifier, u64)  <-- this is the cache key
  2: block_type       (7=index, 8=filter, 9=data, ...)
  3: block_size       (bytes, u64)
  ... (remaining columns not needed)

Kia binary format (25 bytes per entry, little-endian):
  8 bytes: timestamp  (u64, milliseconds)
  1 byte:  command    (0 = get)
  8 bytes: key        (u64)
  4 bytes: size       (u32)
  4 bytes: ttl        (u32, 0 = no ttl)

Usage:
  python3 rocksdb_trace_to_kia.py --input /tmp/trace_human.txt --output /tmp/trace.bin
  python3 rocksdb_trace_to_kia.py --input /tmp/trace_human.txt --output /tmp/trace_data_only.bin --data-blocks-only
"""

import argparse
import struct
import sys

KIA_ENTRY_FORMAT = "<Q B Q I I"  # 8 + 1 + 8 + 4 + 4 = 25 bytes
KIA_ENTRY_SIZE = 25
COMMAND_GET = 0

# RocksDB block types (from trace_replay.h)
BLOCK_TYPE_DATA = 9


def convert(input_path: str, output_path: str, data_blocks_only: bool):
    entries_written = 0
    entries_skipped = 0

    with open(input_path, "r") as fin, open(output_path, "wb") as fout:
        for lineno, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue

            parts = line.split(",")
            if len(parts) < 4:
                print(f"Warning: line {lineno} has fewer than 4 columns, skipping")
                continue

            try:
                timestamp_us = int(parts[0])
                block_id     = int(parts[1])
                block_type   = int(parts[2])
                block_size   = int(parts[3])
            except ValueError:
                print(f"Warning: could not parse line {lineno}, skipping")
                continue

            if data_blocks_only and block_type != BLOCK_TYPE_DATA:
                entries_skipped += 1
                continue

            # Kia uses milliseconds for timestamp
            timestamp_ms = timestamp_us // 1000

            entry = struct.pack(
                KIA_ENTRY_FORMAT,
                timestamp_ms,   # u64 timestamp
                COMMAND_GET,    # u8  command (all block cache accesses are reads)
                block_id,       # u64 key
                block_size,     # u32 size
                0,              # u32 ttl (no ttl)
            )
            fout.write(entry)
            entries_written += 1

            if entries_written % 1_000_000 == 0:
                print(f"  {entries_written:,} entries written...", flush=True)

    print(f"Done. Written: {entries_written:,}  Skipped: {entries_skipped:,}")
    print(f"Output: {output_path}  ({entries_written * KIA_ENTRY_SIZE / 1024 / 1024:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(
        description="Convert RocksDB human-readable block cache trace to Kia binary format"
    )
    parser.add_argument("--input",  required=True, help="Path to trace_human.txt")
    parser.add_argument("--output", required=True, help="Path to output .bin file")
    parser.add_argument(
        "--data-blocks-only",
        action="store_true",
        help="Only include data block accesses (block_type=9), skip index/filter blocks",
    )
    args = parser.parse_args()

    print(f"Converting {args.input} -> {args.output}")
    if args.data_blocks_only:
        print("Filtering: data blocks only (block_type=9)")

    convert(args.input, args.output, args.data_blocks_only)


if __name__ == "__main__":
    main()
