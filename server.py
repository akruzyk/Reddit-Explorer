from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import time
import traceback
import redis
import json
from pathlib import Path

app = Flask(__name__)
CORS(app)

DB_PATH = 'reddit_communities.db'

def get_db_connection():
    """Get database connection with proper settings"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This allows us to access columns by name
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

@app.route('/api/communities')
def get_communities():
    try:
        # Get parameters with defaults
        tier = request.args.get('tier', 'all')
        search = request.args.get('search', '').strip()
        search_mode = request.args.get('mode', 'all')
        sort_by = request.args.get('sort', 'subscribers')
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)
        nsfw_only = request.args.get('nsfw_only', '').lower() == 'true'

        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Base query and params
        base_query = "FROM communities WHERE 1=1"
        count_query = f"SELECT COUNT(*) {base_query}"
        params = []
        
        # Tier filter
        if tier == 'major':
            tier_params = [1000000]
            tier_clause = "subscribers >= ?"
        elif tier == 'rising':
            tier_params = [100000, 1000000]
            tier_clause = "subscribers >= ? AND subscribers < ?"
        elif tier == 'growing':
            tier_params = [10000, 100000]
            tier_clause = "subscribers >= ? AND subscribers < ?"
        elif tier == 'emerging':
            tier_params = [1000, 10000]
            tier_clause = "subscribers >= ? AND subscribers < ?"
        else:
            tier_params = []
            tier_clause = ""
        
        # Search handling
        search_uses_fts = False
        if search:
            if search_mode == 'name':
                base_query += f" AND display_name LIKE ?"
                params = [f"%{search}%"] + tier_params
                if tier_clause:
                    base_query += f" AND {tier_clause}"
                count_query = f"SELECT COUNT(*) {base_query}"
            elif search_mode == 'description':
                base_query += " AND public_description LIKE ?"
                params = [f"%{search}%"] + tier_params
                if tier_clause:
                    base_query += f" AND {tier_clause}"
                count_query = f"SELECT COUNT(*) {base_query}"
            else:  # 'all' => FTS
                search_uses_fts = True
                # Restrict FTS to public_description only
                if tier != 'all':
                    base_query = (
                        "FROM communities c "
                        "INNER JOIN communities_fts ON c.id = communities_fts.rowid "
                        f"WHERE communities_fts.public_description MATCH ?"
                    )
                    if tier_clause:
                        base_query += f" AND {tier_clause}"
                    params = [search] + tier_params
                    count_query = f"SELECT COUNT(*) {base_query}"
                else:
                    base_query = (
                        "FROM communities c "
                        "INNER JOIN communities_fts ON c.id = communities_fts.rowid "
                        f"WHERE communities_fts.public_description MATCH ?"
                    )
                    params = [search]
                    count_query = f"SELECT COUNT(*) {base_query}"
        else:
            params = tier_params
            if tier_clause:
                base_query += f" AND {tier_clause}"
                count_query = f"SELECT COUNT(*) {base_query}"
        
        if nsfw_only:
            base_query += " AND over18 = 1"

        # Total count
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        
        # Sort clause
        sort_clause = "ORDER BY subscribers DESC"
        if sort_by == 'subscribers_asc':
            sort_clause = "ORDER BY subscribers ASC"
        elif sort_by == 'name':
            sort_clause = "ORDER BY display_name ASC"
        elif sort_by == 'name_desc':
            sort_clause = "ORDER BY display_name DESC"
        elif sort_by == 'created':
            sort_clause = "ORDER BY created_date DESC"
        elif sort_by == 'created_desc':
            sort_clause = "ORDER BY created_date ASC"
        
        # Pagination
        offset = (page - 1) * per_page
        if search_uses_fts:
            select_query = f"SELECT DISTINCT c.*, c.comment_count_july25 AS comment_count {base_query} {sort_clause} LIMIT ? OFFSET ?"
        else:
            select_query = f"SELECT *, comment_count_july25 AS comment_count {base_query} {sort_clause} LIMIT ? OFFSET ?"
        
        query_params = params + [per_page, offset]
        cursor.execute(select_query, query_params)
        rows = cursor.fetchall()
        
        
        # Convert rows to JSON
        result_data = []
        for row in rows:
            item = {
                'display_name': row['display_name'] or '',
                'url': row['url'] or '',
                'subscribers': int(row['subscribers'] or 0),
                'created_date': row['created_date'] or '',
                'public_description': row['public_description'] or '',
                'description': row['description'] or '',
                'created_date': row['created_date'] or '',
                'over18': int(row['over18'] or 0),
                'subreddit_type': row['subreddit_type'] or '',
                'name': row['name'] or '',
                'title': row['title'] or '',
                'comment_count': int(row['comment_count'] or 0), # Add this line
            }
            result_data.append(item)
        
        total_pages = max(1, (total + per_page - 1) // per_page)
        conn.close()
        
        return jsonify({
            'data': result_data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages
            }
        })
        
    except Exception as e:
        print(f"Error in /api/communities: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/api/stats')
def get_stats():
    try:
        tier = request.args.get('tier', 'all')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query based on tier
        base_query = "SELECT COUNT(*), SUM(subscribers), AVG(subscribers) FROM communities WHERE 1=1"
        params = []
        
        if tier == 'major':
            base_query += " AND subscribers >= ?"
            params.append(1000000)
        elif tier == 'rising':
            base_query += " AND subscribers >= ? AND subscribers < ?"
            params.extend([100000, 1000000])
        elif tier == 'growing':
            base_query += " AND subscribers >= ? AND subscribers < ?"
            params.extend([10000, 100000])
        elif tier == 'emerging':
            base_query += " AND subscribers >= ? AND subscribers < ?"
            params.extend([1000, 10000])
        
        cursor.execute(base_query, params)
        result = cursor.fetchone()
        
        total_count = result[0] or 0
        total_subs = result[1] or 0
        avg_subs = result[2] or 0
        
        conn.close()
        
        return jsonify({
            'total': total_count,
            'total_subscribers': int(total_subs),
            'avg_subscribers': int(avg_subs)
        })
        
    except Exception as e:
        print(f"Error in /api/stats: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    db_ok, message = check_database()
    
    if not db_ok:
        return jsonify({
            'status': 'error',
            'message': message,
            'total_communities': 0
        }), 500
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get basic stats
        cursor.execute("SELECT COUNT(*) FROM communities")
        total = cursor.fetchone()[0]
        
        # Get column info
        cursor.execute("PRAGMA table_info(communities)")
        columns = [col[1] for col in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'message': message,
            'total_communities': total,
            'columns': columns
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': f'Database error: {str(e)}',
            'total_communities': 0
        }), 500

@app.route('/api/debug')
def debug_info():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get sample data
        cursor.execute("SELECT * FROM communities LIMIT 1")
        row = cursor.fetchone()
        sample_data = dict(row) if row else None
        
        # Get table info
        cursor.execute("PRAGMA table_info(communities)")
        table_info = cursor.fetchall()
        
        # Get database file size
        db_size = Path(DB_PATH).stat().st_size / (1024*1024) if Path(DB_PATH).exists() else 0
        
        conn.close()
        
        return jsonify({
            'database_path': DB_PATH,
            'database_size_mb': round(db_size, 2),
            'table_info': table_info,
            'sample_data': sample_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search-performance')
def search_performance():
    """Test endpoint to check search performance"""
    try:
        search_term = request.args.get('term', 'technology')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Test different search methods
        results = {}
        
        # 1. LIKE search on display name
        start = time.time()
        cursor.execute("SELECT COUNT(*) FROM communities WHERE display_name LIKE ?", [f'%{search_term}%'])
        count = cursor.fetchone()[0]
        results['like_name'] = {
            'count': count,
            'time_ms': round((time.time() - start) * 1000, 2)
        }
        
        # 2. LIKE search on descriptions 
        start = time.time()
        cursor.execute("SELECT COUNT(*) FROM communities WHERE public_description LIKE ?", 
                      [f'%{search_term}%', f'%{search_term}%'])
        count = cursor.fetchone()[0]
        results['like_description'] = {
            'count': count,
            'time_ms': round((time.time() - start) * 1000, 2)
        }
        
        # 3. FTS search
        start = time.time()
        cursor.execute("SELECT COUNT(*) FROM communities_fts WHERE communities_fts MATCH ?", [search_term])
        count = cursor.fetchone()[0]
        results['fts'] = {
            'count': count,
            'time_ms': round((time.time() - start) * 1000, 2)
        }
        
        conn.close()
        
        return jsonify({
            'search_term': search_term,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Check database on startup
    db_ok, message = check_database()
    print(f"🗄️ Database status: {message}")
    
    if not db_ok:
        print("❌ Database not ready. Please run the migration script first:")
        print("   python migrate_to_sqlite.py")
    else:
        print("✅ Database ready, starting server...")
    
    app.run(debug=True, port=5000, threaded=True)
