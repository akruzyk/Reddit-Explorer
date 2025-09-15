import zstandard as zst
import json
import csv
import sys
import logging
import os
import re
from tqdm import tqdm
from datetime import datetime, timedelta

# Configure logging
def setup_logging(year, month):
    log_file = f'skipped_lines_{year}_{month:02d}.log'
    logging.basicConfig(
        filename=log_file,
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return log_file

# Calculate UTC timestamp range for a given year/month
def get_month_utc_range(year, month):
    start_date = datetime(year, month, 1)
    next_month = (month % 12) + 1
    next_year = year + (month // 12)
    end_date = datetime(next_year, next_month, 1) - timedelta(seconds=1)
    return int(start_date.timestamp()), int(end_date.timestamp())

# Parse command-line arguments
if len(sys.argv) < 2:
    print("Usage: python zst_to_csv.py PATH_TO_ZST_FILE [SUBREDDIT]")
    sys.exit(1)

ZST_FILE = sys.argv[1]
TARGET_SUBREDDIT = sys.argv[2].lower() if len(sys.argv) > 2 else None

# Extract year and month from filename
match = re.match(r'.*RC_(\d{4})-(\d{2})\.zst$', ZST_FILE)
if not match:
    raise ValueError("Filename must be in format RC_YYYY-MM.zst")
year, month = int(match.group(1)), int(match.group(2))

# Setup logging
log_file = setup_logging(year, month)

# Output CSV file
csv_file = ZST_FILE.replace('.zst', '.csv')

# Get UTC timestamp range for the month
utc_start, utc_end = get_month_utc_range(year, month)

# Process .zst file and write to CSV
file_size = os.path.getsize(ZST_FILE)
try:
    with open(ZST_FILE, 'rb') as fh, open(csv_file, 'w', newline='', encoding='utf-8') as csv_fh:
        dctx = zst.ZstdDecompressor(max_window_size=2147483648)
        csv_writer = csv.writer(csv_fh, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(['subreddit', 'created_utc', 'date', 'body', 'id', 'author'])
        
        with dctx.stream_reader(fh) as reader:
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Processing {os.path.basename(ZST_FILE)}")
            buffer = b""
            while True:
                chunk = reader.read(8192)
                if not chunk:
                    break
                buffer += chunk
                pbar.update(len(chunk))
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        data = json.loads(line.decode('utf-8', errors='ignore'))
                        subreddit = data.get('subreddit', '').lower()
                        created_utc = data.get('created_utc', 0)
                        # Convert created_utc to readable date
                        try:
                            created_date = datetime.utcfromtimestamp(int(created_utc)).strftime('%Y-%m-%d')
                        except (ValueError, TypeError):
                            created_date = "Invalid"
                        # Filter by subreddit and month (optional)
                        if subreddit and (TARGET_SUBREDDIT is None or subreddit == TARGET_SUBREDDIT) and utc_start <= int(created_utc) <= utc_end:
                            csv_writer.writerow([
                                subreddit,
                                created_utc,
                                created_date,
                                data.get('body', '').replace('\n', ' ').replace('\r', ' '),  # Clean newlines
                                data.get('id', ''),
                                data.get('author', '')
                            ])
                            logging.info(f"Wrote comment for {subreddit}: {data.get('id', 'unknown')}")
                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        logging.warning(f"Skipped line: {line[:100].decode('utf-8', errors='ignore')}... | Error: {e}")
            pbar.close()
except zstd.ZstdError as e:
    print(f"❌ Zstandard decompression error: {e}")
    sys.exit(1)

print(f"Conversion complete. CSV saved to {csv_file}")