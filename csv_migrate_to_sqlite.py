#!/usr/bin/env python3
"""
Migrate subreddit CSVs (with extended fields) into SQLite.
- Lists folders in current directory so you can pick one
- Handles boolean and numeric normalization
- Cleans text values
- Imports all CSVs in the chosen folder
"""

import sqlite3
import csv
import re
import time
import sys
from pathlib import Path
import traceback

# ---------- CONFIG ----------
DB_PATH = Path("reddit_communities.db")
BATCH_SIZE = 1000


def create_database_schema(db_path):
    """Create the SQLite database and table schema"""
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
            wiki_enabled INTEGER
        )
    """)

    # Indexes
    cursor.execute("CREATE INDEX idx_subscribers ON communities(subscribers)")
    cursor.execute("CREATE INDEX idx_display_name ON communities(display_name)")
    cursor.execute("CREATE INDEX idx_created_date ON communities(created_date)")

    # FTS for fast searching
    cursor.execute("""
        CREATE VIRTUAL TABLE communities_fts USING fts5(
            display_name, 
            public_description, 
            description,
            title,
            content='communities',
            content_rowid='id'
        )
    """)

    # Triggers
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
    print("✅ Database schema created successfully")


def insert_batch(cursor, batch_data, fieldnames):
    """Insert a batch of data into the database (extended schema)"""
    if not batch_data:
        return

    column_mapping = {
        'display_name': 'display_name',
        'created_date': 'created_date',
        'public_description': 'public_description',
        'description': 'description',
        'subscribers': 'subscribers',
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
        'wiki_enabled': 'wiki_enabled'
    }

    available_columns = set(fieldnames)
    db_columns = []
    placeholders = []

    for csv_col, db_col in column_mapping.items():
        if csv_col in available_columns:
            db_columns.append(db_col)
            placeholders.append('?')

    query = f"""
        INSERT OR REPLACE INTO communities ({', '.join(db_columns)})
        VALUES ({', '.join(placeholders)})
    """

    batch_values = []
    for row in batch_data:
        values = []
        for csv_col, db_col in column_mapping.items():
            if csv_col in available_columns:
                values.append(row.get(csv_col))
        batch_values.append(values)

    cursor.executemany(query, batch_values)


def load_csv_to_sqlite(filename, db_path):
    """Load a CSV file directly into SQLite with extended cleaning"""
    print(f"📂 Loading {filename.name} into database...")
    start_time = time.time()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    line_count = 0
    batch_data = []

    try:
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)

            fieldnames = [h.strip().replace('"', '') for h in reader.fieldnames]
            reader.fieldnames = fieldnames

            for row in reader:
                line_count += 1
                clean_row = {}

                # Clean values
                for key, value in row.items():
                    if value is None:
                        clean_row[key] = None
                    else:
                        clean_value = str(value).strip().replace('"', '')
                        clean_value = re.sub(r'\s+', ' ', clean_value)
                        clean_row[key] = clean_value if clean_value else None

                # Normalize numeric + boolean fields
                try:
                    clean_row['subscribers'] = int(clean_row.get('subscribers', 0) or 0)
                except:
                    clean_row['subscribers'] = 0

                over18_val = (clean_row.get('over18') or "").strip().lower()
                clean_row['over18'] = 1 if over18_val in ("true", "1", "yes") else 0

                bool_fields = [
                    'all_original_content', 'allow_discovery', 'allow_images',
                    'allow_galleries', 'allow_polls', 'allow_videos',
                    'allow_videogifs', 'wiki_enabled'
                ]
                for bf in bool_fields:
                    val = (clean_row.get(bf) or "").strip().lower()
                    clean_row[bf] = 1 if val in ("true", "1", "yes") else 0

                int_fields = ['num_posts', 'num_comments']
                for nf in int_fields:
                    try:
                        clean_row[nf] = int(clean_row.get(nf) or 0)
                    except:
                        clean_row[nf] = 0

                if clean_row.get('created_date'):
                    clean_row['created_date'] = clean_row['created_date'].split(' ')[0]

                batch_data.append(clean_row)

                if len(batch_data) >= BATCH_SIZE:
                    insert_batch(cursor, batch_data, fieldnames)
                    batch_data = []

                if line_count % 10000 == 0:
                    print(f"   Processed {line_count:,} lines...")

            if batch_data:
                insert_batch(cursor, batch_data, fieldnames)

        conn.commit()

    except Exception as e:
        print(f"   ❌ Error reading {filename}: {e}")
        print(traceback.format_exc())
        conn.rollback()
        line_count = 0
    finally:
        conn.close()

    end_time = time.time()
    print(f"   ✅ Loaded {line_count:,} rows from {filename.name} in {end_time - start_time:.2f}s")
    return line_count


def choose_input_folder() -> Path:
    """Show available folders in current dir and let user pick one"""
    cwd = Path(".")
    folders = [f for f in cwd.iterdir() if f.is_dir()]

    if not folders:
        print("❌ No folders found in current directory.")
        sys.exit(1)

    print("\n📂 Available folders:")
    for idx, folder in enumerate(folders, 1):
        print(f"  [{idx}] {folder.name}")

    choice = input("\nEnter folder number to import CSVs from: ").strip()
    try:
        idx = int(choice)
        if 1 <= idx <= len(folders):
            return folders[idx - 1]
        else:
            print("❌ Invalid selection.")
            sys.exit(1)
    except:
        print("❌ Invalid input.")
        sys.exit(1)


def migrate_all_data(input_dir: Path):
    """Migrate all CSV files in chosen folder into SQLite"""
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"❌ Input folder does not exist: {input_dir}")
        return

    if DB_PATH.exists():
        print(f"🗑️ Removing existing database: {DB_PATH}")
        DB_PATH.unlink()

    create_database_schema(DB_PATH)

    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        print(f"❌ No CSV files found in {input_dir}")
        return

    total_records = 0
    start_time = time.time()

    for csv_file in csv_files:
        records = load_csv_to_sqlite(csv_file, DB_PATH)
        total_records += records

    end_time = time.time()
    print("\n📊 Migration Summary:")
    print(f"   Total records: {total_records:,}")
    print(f"   Total time: {end_time - start_time:.2f}s")
    print(f"   Database size: {DB_PATH.stat().st_size / (1024*1024):.1f} MB")


if __name__ == "__main__":
    folder = choose_input_folder()
    migrate_all_data(folder)
