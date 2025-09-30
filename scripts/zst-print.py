#!/usr/bin/env python3
"""
Print the first N lines from a .zst file with robust error handling.
"""

import zstandard as zst
import io
import sys
from pathlib import Path

def print_first_lines(file_path, num_lines=100):
    try:
        # Set a reasonable max_window_size (2 GB, matching your main script)
        dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        with open(file_path, "rb") as compressed_file:
            with dctx.stream_reader(compressed_file) as reader:
                text_reader = io.TextIOWrapper(reader, encoding="utf-8", errors="ignore")
                for i, line in enumerate(text_reader, 1):
                    print(line.rstrip())
                    if i >= num_lines:
                        break
                print(f"\n✅ Printed {min(i, num_lines)} lines from {file_path}")
    except zstd.ZstdError as e:
        print(f"❌ Zstandard decompression error: {e}")
        print("Possible causes: Corrupted .zst file, insufficient memory, or incompatible compression settings.")
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        print("Check file path and ensure it is a valid .zst file.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python zst-print.py PATH_TO_FILE NUM_LINES")
        sys.exit(1)

    file_path = sys.argv[1]
    num_lines = int(sys.argv[2])

    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    print_first_lines(file_path, num_lines)