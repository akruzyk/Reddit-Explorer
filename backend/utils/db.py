import sqlite3
from pathlib import Path

# Absolute path to Reddit-Explorer/reddit_communities.db
DB_PATH = str(Path('/Users/akruzyk/Programming/Reddit-Explorer/reddit_communities.db'))

def get_db_connection():
    """Get database connection with proper settings"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def check_database():
    """Check if database exists and has data"""
    if not Path(DB_PATH).exists():
        return False, "Database file not found"
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM communities")
        count = cursor.fetchone()[0]
        conn.close()
        
        if count == 0:
            return False, "Database is empty"
        
        return True, f"Database ready with {count:,} communities"
    except Exception as e:
        return False, f"Database error: {str(e)}"
