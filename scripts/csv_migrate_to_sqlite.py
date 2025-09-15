#!/usr/bin/env python3
"""
Migrate subreddit CSVs into SQLite.
- all_subreddits_with_comments.csv: metadata + comment_count_july25 -> communities, comment_history
- subreddits-07-25.csv: subreddit,comment_count -> comment_history
- 2025-07-comments.csv: timestamp,subreddit -> comment_history (year/month/week/day/hour)
"""

import sqlite3
import csv
import re
import time
import sys
from pathlib import Path
import traceback
from datetime import datetime
import glob
import pandas as pd
from tqdm import tqdm

DB_PATH = Path("reddit_communities.db")
BATCH_SIZE = 1000
DEFAULT_FOLDER = Path("data")

def extract_date_from_filename(filename: Path) -> datetime:
    match = re.search(r'(\d{4}-\d{2}(?:-\d{2})?)', filename.name)
    return datetime.strptime(match.group(1), '%Y-%m') if match else datetime.min

def create_database_schema(db_path):
    print("🗄️ Creating database schema...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS communities")
    cursor.execute("""
        CREATE TABLE communities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            display_name TEXT,
            created_date TEXT,
            public_description TEXT,
            description TEXT,
            subscribers INTEGER DEFAULT 0,
            subscribers_snapshot_date TEXT,
            url TEXT,
            over18 INTEGER DEFAULT 0,
            title TEXT,
            num_posts INTEGER,
            num_posts_updated_at TEXT,
            num_comments INTEGER,
            num_comments_updated_at TEXT,
            earliest_post_at TEXT,
            earliest_comment_at TEXT,
            all_original_content INTEGER,
            allow_discovery INTEGER,
            allow_images INTEGER,
            allow_galleries INTEGER,
            allow_polls INTEGER,
            allow_videos INTEGER,
            allow_videogifs INTEGER,
            header_title TEXT,
            name TEXT UNIQUE,
            retrieved_on TEXT,
            submission_type TEXT,
            submit_link_label TEXT,
            submit_text TEXT,
            submit_text_label TEXT,
            subreddit_type TEXT,
            suggested_comment_sort TEXT,
            wiki_enabled INTEGER,
            category TEXT
        )
    """)

    cursor.execute("DROP TABLE IF EXISTS comment_history")
    cursor.execute("""
            CREATE TABLE comment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subreddit TEXT,                
                year INTEGER,
                month INTEGER,
                week INTEGER,
                day INTEGER,
                hour INTEGER,
                comment_count INTEGER,
                period_date TEXT,  -- YYYY-MM-DD format for easy date operations
                UNIQUE(subreddit, year, month, week, day, hour)
            )
        """)

    cursor.execute("CREATE INDEX idx_subscribers ON communities(subscribers)")
    cursor.execute("CREATE INDEX idx_subscribers_snapshot_date ON communities(subscribers_snapshot_date)")
    cursor.execute("CREATE INDEX idx_display_name ON communities(display_name)")
    cursor.execute("CREATE INDEX idx_created_date ON communities(created_date)")
    cursor.execute("CREATE INDEX idx_over18 ON communities(over18)")
    cursor.execute("CREATE INDEX idx_category ON communities(category)")
    cursor.execute("CREATE INDEX idx_comment_history_subreddit ON comment_history(subreddit)")
    cursor.execute("CREATE INDEX idx_comment_history_year ON comment_history(year)")
    cursor.execute("CREATE INDEX idx_comment_history_month ON comment_history(year, month)")
    cursor.execute("CREATE INDEX idx_comment_history_week ON comment_history(year, week)")
    cursor.execute("CREATE INDEX idx_comment_history_day ON comment_history(year, month, day)")
    cursor.execute("CREATE INDEX idx_comment_history_hour ON comment_history(year, month, day, hour)")
    cursor.execute("CREATE INDEX idx_comment_history_date ON comment_history(period_date)")

    cursor.execute("""
        CREATE VIRTUAL TABLE communities_fts USING fts5(
            display_name, public_description, description, title,
            content='communities', content_rowid='id'
        )
    """)

    cursor.execute("""
        CREATE TRIGGER communities_ai AFTER INSERT ON communities BEGIN
            INSERT INTO communities_fts(rowid, display_name, public_description, description, title)
            VALUES (new.id, new.display_name, new.public_description, new.description, new.title);
        END
    """)
    cursor.execute("""
        CREATE TRIGGER communities_ad AFTER DELETE ON communities BEGIN
            INSERT INTO communities_fts(communities_fts, rowid, display_name, public_description, description, title)
            VALUES('delete', old.id, old.display_name, old.public_description, old.description, old.title);
        END
    """)
    cursor.execute("""
        CREATE TRIGGER communities_au AFTER UPDATE ON communities BEGIN
            INSERT INTO communities_fts(communities_fts, rowid, display_name, public_description, description, title)
            VALUES('delete', old.id, old.display_name, old.public_description, old.description, old.title);
            INSERT INTO communities_fts(rowid, display_name, public_description, description, title)
            VALUES (new.id, new.display_name, new.public_description, new.description, new.title);
        END
    """)

    conn.commit()
    conn.close()
    print("✅ Schema created")

def insert_communities_batch(cursor, batch_data, fieldnames):
    if not batch_data or not fieldnames:
        return

    column_mapping = {
        'display_name': 'display_name',
        'created_date': 'created_date',
        'public_description': 'public_description',
        'description': 'description',
        'subscribers': 'subscribers',
        'subscribers_snapshot_date': 'subscribers_snapshot_date',
        'url': 'url',
        'over18': 'over18',
        'title': 'title',
        'num_posts': 'num_posts',
        'num_posts_updated_at': 'num_posts_updated_at',
        'num_comments': 'num_comments',
        'num_comments_updated_at': 'num_comments_updated_at',
        'earliest_post_at': 'earliest_post_at',
        'earliest_comment_at': 'earliest_comment_at',
        'all_original_content': 'all_original_content',
        'allow_discovery': 'allow_discovery',
        'allow_images': 'allow_images',
        'allow_galleries': 'allow_galleries',
        'allow_polls': 'allow_polls',
        'allow_videos': 'allow_videos',
        'allow_videogifs': 'allow_videogifs',
        'header_title': 'header_title',
        'name': 'name',
        'retrieved_on': 'retrieved_on',
        'submission_type': 'submission_type',
        'submit_link_label': 'submit_link_label',
        'submit_text': 'submit_text',
        'submit_text_label': 'submit_text_label',
        'subreddit_type': 'subreddit_type',
        'suggested_comment_sort': 'suggested_comment_sort',
        'wiki_enabled': 'wiki_enabled',
        'category': 'category'
    }

    available_columns = set(fieldnames)
    db_columns = [db_col for csv_col, db_col in column_mapping.items() if csv_col in available_columns]
    if not db_columns:
        print(f"⚠️ No matching columns for communities table in CSV with fields: {fieldnames}")
        return

    placeholders = ['?' for _ in db_columns]
    query = f"INSERT OR REPLACE INTO communities ({', '.join(db_columns)}) VALUES ({', '.join(placeholders)})"

    batch_values = []
    for row in batch_data:
        values = []
        for csv_col, db_col in column_mapping.items():
            if csv_col in available_columns:
                val = row.get(csv_col)
                if csv_col == 'subscribers_snapshot_date' and not val:
                    val = row.get('retrieved_on', '').split(' ')[0] if row.get('retrieved_on') else None
                values.append(val)
        batch_values.append(values)

    cursor.executemany(query, batch_values)

def insert_comment_history_batch(cursor, batch_data):
    if not batch_data:
        return
    query = """
        INSERT OR REPLACE INTO comment_history 
        (subreddit, year, month, week, day, hour, comment_count, period_date) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(query, batch_data)

def load_community_csv(filename, db_path):
    print(f"📂 Loading community CSV {filename.name}...")
    file_size = filename.stat().st_size
    avg_row_size = 1000
    estimated_rows = file_size // avg_row_size if file_size > 0 else 1000

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable dictionary-like row access
    cursor = conn.cursor()

    line_count = 0
    batch_data = []

    try:
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            fieldnames = [h.strip().replace('"', '') for h in reader.fieldnames]
            reader.fieldnames = fieldnames

            with tqdm(total=estimated_rows, desc=f"Processing {filename.name}", unit="rows") as pbar:
                for row in reader:
                    line_count += 1
                    clean_row = {}

                    # Clean and normalize CSV data
                    for key, value in row.items():
                        clean_value = str(value).strip().replace('"', '') if value else None
                        clean_value = re.sub(r'\s+', ' ', clean_value) if clean_value else None
                        clean_row[key] = clean_value

                    # Convert numeric and boolean fields
                    clean_row['subscribers'] = int(clean_row.get('subscribers', 0) or 0) or 0
                    clean_row['over18'] = 1 if (clean_row.get('over18') or "").lower() in ("true", "1", "yes") else 0

                    bool_fields = ['all_original_content', 'allow_discovery', 'allow_images', 'allow_galleries', 'allow_polls', 'allow_videos', 'allow_videogifs', 'wiki_enabled']
                    for bf in bool_fields:
                        clean_row[bf] = 1 if (clean_row.get(bf) or "").lower() in ("true", "1", "yes") else 0

                    int_fields = ['num_posts', 'num_comments']
                    for nf in int_fields:
                        clean_row[nf] = int(clean_row.get(nf) or 0) or 0

                    # Normalize date format
                    if clean_row.get('created_date'):
                        clean_row['created_date'] = clean_row['created_date'].split(' ')[0]

                    # Derive category based on name/description
                    name = (clean_row.get('display_name') or '').lower()
                    desc = (clean_row.get('public_description') or clean_row.get('description') or '').lower()
                    clean_row['category'] = (
                        'nsfw' if clean_row.get('over18') == 1 or 'nsfw' in name or 'nsfw' in desc else
                        'gaming' if 'gaming' in name or 'game' in name or 'game' in desc else
                        'technology' if 'tech' in name or 'programming' in name or 'code' in name or 'technology' in desc else
                        'discussion' if 'ask' in name or 'discussion' in name or 'discuss' in desc else
                        'humor' if 'humor' in name or 'meme' in name or 'funny' in desc else
                        'images' if 'photo' in name or 'image' in name or 'photography' in desc else
                        'news' if 'news' in name or 'event' in desc else
                        'creative' if 'art' in name or 'music' in name or 'writing' in desc else
                        'support' if 'support' in name or 'help' in desc else
                        'all'
                    )

                    batch_data.append(clean_row)

                    if len(batch_data) >= BATCH_SIZE:
                        insert_communities_batch(cursor, batch_data, fieldnames)
                        batch_data = []
                        pbar.update(BATCH_SIZE)

                if batch_data:
                    insert_communities_batch(cursor, batch_data, fieldnames)
                    pbar.update(len(batch_data))

                pbar.update(line_count - pbar.n)

        conn.commit()

    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        print(traceback.format_exc())
        conn.rollback()
        line_count = 0
    finally:
        conn.close()

    print(f"✅ Loaded {line_count:,} rows from {filename.name}")
    return line_count

def load_monthly_comment_csv(filename, db_path, year, month):
    print(f"📂 Loading monthly comment CSV {filename.name}...")
    file_size = filename.stat().st_size
    avg_row_size = 100
    estimated_rows = file_size // avg_row_size if file_size > 0 else 1000

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.execute("PRAGMA cache_size = -20000")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    line_count = 0
    batch_data = []

    try:
        cursor.execute("SELECT id, name FROM communities WHERE subscribers >= 1000")
        subreddit_ids = {row['name']: row['id'] for row in cursor.fetchall()}

        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            fieldnames = [h.strip().replace('"', '') for h in reader.fieldnames]

            if 'subreddit' not in fieldnames or 'comment_count' not in fieldnames:
                print(f"⚠️ Skipping {filename.name}: Missing 'subreddit' or 'comment_count' columns")
                return 0

            with tqdm(total=estimated_rows, desc=f"Processing {filename.name}", unit="rows") as pbar:
                for row in reader:
                    line_count += 1
                    subreddit = row.get('subreddit')
                    if not subreddit:
                        continue

                    subreddit_id = subreddit_ids.get(subreddit)
                    if not subreddit_id:
                        continue

                    try:
                        comment_count = int(row.get('comment_count') or 0)
                    except:
                        comment_count = 0

                    if comment_count > 0:
                        # Use None for week/day/hour/period_date for monthly totals
                        batch_data.append((subreddit, year, month, None, None, None, comment_count, None))

                    if len(batch_data) >= BATCH_SIZE:
                        insert_comment_history_batch(cursor, batch_data)
                        batch_data = []

                    pbar.update(1)

                if batch_data:
                    insert_comment_history_batch(cursor, batch_data)

                pbar.update(line_count - pbar.n)

        conn.commit()
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA locking_mode = NORMAL")

    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        print(traceback.format_exc())
        conn.rollback()
        line_count = 0
    finally:
        conn.close()

    print(f"✅ Loaded {line_count:,} rows from {filename.name}")
    return line_count

def load_individual_comments_csv(filename, db_path):
    print(f"📂 Loading comments CSV {filename.name}...")
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -50000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")

    cursor = conn.cursor()
    
    # Use a dictionary to aggregate counts by time period and subreddit
    aggregated_data = {}
    line_count = 0
    chunks_processed = 0
    
    try:
        chunk_size = 100000
        chunks = pd.read_csv(filename, chunksize=chunk_size, encoding='utf-8', engine='python', 
                           on_bad_lines='skip', names=['timestamp', 'subreddit'])
        
        with tqdm(desc=f"Processing {filename.name}", unit="chunks") as pbar:
            for chunk in chunks:
                chunks_processed += 1
                
                if chunks_processed % 10 == 0:
                    print(f"📊 Processed {chunks_processed} chunks (~{chunks_processed * chunk_size:,} rows)")
                
                for _, row in chunk.iterrows():
                    line_count += 1
                    try:
                        timestamp = int(row['timestamp'])
                        subreddit = str(row['subreddit']).strip()
                        if not subreddit:
                            continue
                            
                        dt = datetime.fromtimestamp(timestamp)
                        year = dt.year
                        month = dt.month
                        week = (dt.day - 1) // 7 + 1
                        day = dt.day
                        hour = dt.hour
                        period_date = dt.strftime('%Y-%m-%d')
                        
                        # Create a unique key for this time period and subreddit
                        period_key = (subreddit, year, month, week, day, hour, period_date)
                        
                        if period_key not in aggregated_data:
                            aggregated_data[period_key] = 0
                        aggregated_data[period_key] += 1
                        
                    except Exception as e:
                        continue
                
                # Periodically flush to database to manage memory
                if chunks_processed % 50 == 0 or len(aggregated_data) > 100000:
                    print(f"🔄 Flushing {len(aggregated_data):,} aggregated records to database...")
                    
                    batch_data = []
                    for (subreddit, year, month, week, day, hour, period_date), count in aggregated_data.items():
                        batch_data.append((subreddit, year, month, week, day, hour, count, period_date))
                    
                    # Insert in batches
                    for i in range(0, len(batch_data), BATCH_SIZE):
                        batch_chunk = batch_data[i:i + BATCH_SIZE]
                        cursor.executemany("""
                            INSERT OR REPLACE INTO comment_history 
                            (subreddit, year, month, week, day, hour, comment_count, period_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, batch_chunk)
                    
                    aggregated_data.clear()
                    conn.commit()
                
                pbar.update(1)
        
        # Insert any remaining data
        if aggregated_data:
            print(f"💾 Inserting final {len(aggregated_data):,} records...")
            batch_data = []
            for (subreddit, year, month, week, day, hour, period_date), count in aggregated_data.items():
                batch_data.append((subreddit, year, month, week, day, hour, count, period_date))
            
            cursor.executemany("""
                INSERT OR REPLACE INTO comment_history 
                (subreddit, year, month, week, day, hour, comment_count, period_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            
            conn.commit()
        
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        
    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")
        print(traceback.format_exc())
        conn.rollback()
    finally:
        conn.close()
    
    print(f"✅ Processed {line_count:,} comments from {filename.name}")
    return line_count


def choose_input_folder() -> Path:
    """Prompt the user to select a folder or use the default."""
    print("🔍 Scanning for folders...")
    sys.stdout.flush()  # Ensure print output is displayed immediately

    try:
        cwd = Path(".").resolve()  # Resolve to absolute path for clarity
        print(f"📍 Current working directory: {cwd}")
        folders = [f for f in cwd.iterdir() if f.is_dir()]
        print(f"📁 Found {len(folders)} folders: {[f.name for f in folders]}")
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Error scanning folders: {e}")
        print(f"⚠️ Falling back to default folder: {DEFAULT_FOLDER}")
        if DEFAULT_FOLDER.exists() and DEFAULT_FOLDER.is_dir():
            return DEFAULT_FOLDER
        print(f"❌ Default folder {DEFAULT_FOLDER} does not exist.")
        sys.exit(1)

    if not folders:
        print(f"❌ No folders found. Using default: {DEFAULT_FOLDER}")
        if DEFAULT_FOLDER.exists() and DEFAULT_FOLDER.is_dir():
            return DEFAULT_FOLDER
        print(f"❌ Default folder {DEFAULT_FOLDER} does not exist.")
        sys.exit(1)

    # Check if running interactively
    if not sys.stdin.isatty():
        print("⚠️ Non-interactive environment detected. Using default folder.")
        if DEFAULT_FOLDER.exists() and DEFAULT_FOLDER.is_dir():
            return DEFAULT_FOLDER
        print(f"❌ Default folder {DEFAULT_FOLDER} does not exist.")
        sys.exit(1)

    print("\n📂 Available folders:")
    for idx, folder in enumerate(folders, 1):
        print(f"  [{idx}] {folder.name}")
    sys.stdout.flush()

    try:
        choice = input("\nEnter folder number (or press Enter for default): ").strip()
        print(f"📥 User entered: '{choice}'")
        sys.stdout.flush()
        if not choice:
            print(f"✅ Using default folder: {DEFAULT_FOLDER}")
            if DEFAULT_FOLDER.exists() and DEFAULT_FOLDER.is_dir():
                return DEFAULT_FOLDER
            print(f"❌ Default folder {DEFAULT_FOLDER} does not exist.")
            sys.exit(1)

        idx = int(choice)
        if 1 <= idx <= len(folders):
            selected_folder = folders[idx - 1]
            print(f"✅ Selected folder: {selected_folder.name}")
            return selected_folder
        print(f"❌ Invalid selection: {idx} (must be 1 to {len(folders)})")
        sys.exit(1)
    except ValueError:
        print(f"❌ Invalid input: '{choice}' (must be a number or empty)")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n❌ User interrupted the process.")
        sys.exit(1)
def migrate_all_data(input_dir: Path, rebuild_db: bool = False):
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"❌ Input folder does not exist: {input_dir}")
        return

    if rebuild_db and DB_PATH.exists():
        print(f"🗑️ Removing database: {DB_PATH}")
        DB_PATH.unlink()

    if not DB_PATH.exists():
        print("🗄️ Database does not exist, creating schema...")
        create_database_schema(DB_PATH)

    all_files = sorted(glob.glob(str(input_dir / "*.csv")), key=lambda f: extract_date_from_filename(Path(f)))
    community_files = [f for f in all_files if 'all_subreddits_with_comments' in Path(f).name.lower()]
    comment_files = [f for f in all_files if 'all_subreddits_with_comments' not in Path(f).name.lower()]

    print(f"📅 Found {len(community_files)} community CSVs and {len(comment_files)} comment CSVs")

    total_records = 0
    total_comments = 0
    start_time = time.time()

    with tqdm(total=len(all_files), desc="Processing all CSVs", unit="files") as pbar:
        for csv_file in community_files:
            records = load_community_csv(Path(csv_file), DB_PATH)
            total_records += records
            pbar.update(1)

        for csv_file in comment_files:
            file_path = Path(csv_file)
            # Extract year/month for dynamic insertion
            file_date = extract_date_from_filename(file_path)
            year = file_date.year
            month = file_date.month
            
            if 'subreddits' in file_path.name.lower():
                records = load_monthly_comment_csv(file_path, DB_PATH, year, month)
                total_records += records
            else:
                records = load_individual_comments_csv(file_path, DB_PATH)
                total_comments += records
            pbar.update(1)

    end_time = time.time()
    print("\n📊 Summary:")
    print(f"   Community records: {total_records:,}")
    print(f"   Comment records: {total_comments:,}")
    print(f"   Time: {end_time - start_time:.2f}s")
    print(f"   DB size: {DB_PATH.stat().st_size / (1024*1024):.1f} MB")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM communities WHERE subscribers >= 1000")
    qualified_subreddits = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM comment_history")
    history_records = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(comment_count), MAX(comment_count), AVG(comment_count) FROM comment_history")
    total_comments_db, max_comments, avg_comments = cursor.fetchone()

    # Fix: Replace period_value/period_type with year/month check
    cursor.execute("SELECT COUNT(DISTINCT year || '-' || printf('%02d', month)) FROM comment_history WHERE year IS NOT NULL AND month IS NOT NULL")
    unique_months = cursor.fetchone()[0]

    # Validation: Compare aggregated hourly counts vs. monthly totals
    print("\n🔍 Validating monthly totals...")
    cursor.execute("""
        SELECT subreddit, year, month, SUM(comment_count) AS agg_monthly
        FROM comment_history 
        WHERE year IS NOT NULL AND month IS NOT NULL
        GROUP BY subreddit, year, month
    """)
    agg_totals = {(row['subreddit'], row['year'], row['month']): row['agg_monthly'] for row in cursor.fetchall()}

    # Load monthly totals from comment_history (for subreddits-XX-YY.csv)
    cursor.execute("""
        SELECT subreddit, year, month, comment_count
        FROM comment_history 
        WHERE week IS NULL AND day IS NULL AND hour IS NULL
    """)
    monthly_totals = {(row['subreddit'], row['year'], row['month']): row['comment_count'] for row in cursor.fetchall()}

    # Compare and log mismatches
    mismatches = []
    for key, agg_count in agg_totals.items():
        subreddit, year, month = key
        monthly_count = monthly_totals.get(key, 0)
        if agg_count != monthly_count:
            mismatches.append(f"⚠️ Mismatch for {subreddit} ({year}-{month:02d}): Aggregated={agg_count:,}, Monthly={monthly_count:,}")
    
    if mismatches:
        print("\n📉 Validation Issues:")
        for mismatch in mismatches:
            print(mismatch)
    else:
        print("✅ All monthly totals match aggregated counts!")

    print(f"\n📈 Stats:")
    print(f"   Subreddits (>=1k subs): {qualified_subreddits:,}")
    print(f"   Comment history records: {history_records:,}")
    print(f"   Total comments: {total_comments_db:,.0f}")
    print(f"   Max comments: {max_comments:,.0f}")
    print(f"   Avg comments: {avg_comments:,.1f}")
    print(f"   Unique months: {unique_months}")

    conn.close()

if __name__ == "__main__":
    folder = choose_input_folder()
    migrate_all_data(folder)