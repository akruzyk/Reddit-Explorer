import zstandard as zst
import json
from sqlite3 import connect
import sys
import logging
import os
import re
from tqdm import tqdm
from datetime import datetime, timedelta
import csv

# Calculate UTC timestamp range for a given year/month
def get_month_utc_range(year, month):
    start_date = datetime(year, month, 1)
    # Get first day of next month, then subtract 1 second
    next_month = (month % 12) + 1
    next_year = year + (month // 12)
    end_date = datetime(next_year, next_month, 1) - timedelta(seconds=1)
    return int(start_date.timestamp()), int(end_date.timestamp())

# Load subreddits from CSV file
def load_subreddits_from_csv(csv_file):
    subreddits_set = set()
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                if row:  # Check if row is not empty
                    subreddits_set.add(row[0].lower().strip())
        logging.info(f"Loaded {len(subreddits_set)} subreddits from CSV")
        return subreddits_set
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        sys.exit(1)

# Increase integer string limit
sys.set_int_max_str_digits(10000)

# Parse command-line arguments
if len(sys.argv) < 2:
    print("Usage: python comment_count.py PATH_TO_ZST_FILE")
    sys.exit(1)

ZST_FILE = sys.argv[1]
CSV_FILE = "/Users/akruzyk/Programming/Reddit-Explorer/scripts/subreddits_over_1000_subscribers_2025.csv"  # Update path if needed
DB_FILE = "/Users/akruzyk/Programming/Reddit-Explorer/reddit_communities.db"

# Load target subreddits from CSV
target_subreddits = load_subreddits_from_csv(CSV_FILE)

# Extract year and month from filename
match = re.match(r'.*RC_(\d{4})-(\d{2})\.zst$', ZST_FILE)
if not match:
    raise ValueError("Filename must be in format RC_YYYY-MM.zst")
year, month = int(match.group(1)), int(match.group(2))

# Get UTC timestamp range for the month
utc_start, utc_end = get_month_utc_range(year, month)

# Connect to SQLite
conn = connect(DB_FILE)
cursor = conn.cursor()

# Drop and create table dynamically
table_name = f"comment_count_{year}_{month:02d}"
cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
cursor.execute(f"""
    CREATE TABLE {table_name} (
        subreddit TEXT PRIMARY KEY,
        month_comment_count INTEGER
    )
""")

# Process .zst file with progress tracking
comment_counts = {}
file_size = os.path.getsize(ZST_FILE)
processed_count = 0
skipped_count = 0

try:
    with open(ZST_FILE, 'rb') as fh:
        dctx = zst.ZstdDecompressor(max_window_size=2147483648)
        with dctx.stream_reader(fh) as reader:
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Processing RC_{year}-{month:02d}.zst")
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
                        
                        # Only count if subreddit is in our target list and within date range
                        if (subreddit in target_subreddits and 
                            utc_start <= int(created_utc) <= utc_end):
                            comment_counts[subreddit] = comment_counts.get(subreddit, 0) + 1
                            processed_count += 1
                        else:
                            skipped_count += 1
                            
                    except (json.JSONDecodeError, ValueError, KeyError) as e:
                        skipped_count += 1
                        continue
            pbar.close()
except zst.ZstdError as e:
    print(f"❌ Zstandard decompression error: {e}")
    sys.exit(1)

# Insert into table
for subreddit, count in comment_counts.items():
    cursor.execute(f"INSERT OR REPLACE INTO {table_name} (subreddit, month_comment_count) VALUES (?, ?)",
                  (subreddit, count))

conn.commit()
conn.close()

print(f"Aggregation complete for RC_{year}-{month:02d}")
print(f"Processed {processed_count} comments, skipped {skipped_count} comments")
print(f"Found {len(comment_counts)} subreddits from the CSV file")
print(f"Total comment counts: {sum(comment_counts.values())}")