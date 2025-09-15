# routes/stats.py
from flask import Blueprint, jsonify, request
import sqlite3
from pathlib import Path

stats_bp = Blueprint('stats', __name__)
DB_PATH = Path(__file__).parent.parent / "reddit_communities.db"

@stats_bp.route("/api/comments/<subreddit>", methods=["GET"])
def get_monthly_comments(subreddit):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Find all tables that match comment_count_YYYY_MM
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'comment_count_%';")
    tables = [row[0] for row in cursor.fetchall()]

    monthly_counts = []
    for table in tables:
        cursor.execute(f"SELECT month_comment_count FROM {table} WHERE subreddit = ?", (subreddit,))
        row = cursor.fetchone()
        if row:
            # table name encodes year-month
            _, year, month = table.split("_")
            monthly_counts.append({"month": f"{year}-{month}", "count": row[0]})

    # Sort by month ascending
    monthly_counts.sort(key=lambda x: x["month"])
    conn.close()
    return jsonify(monthly_counts)
