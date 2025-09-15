from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import sqlite3
import time
import traceback
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

@app.route('/')
def serve_index():
    return send_from_directory('.', 'rddit.html')

# Add this route to serve other static files if needed
@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('.', path)

@app.route('/api/communities')
def get_communities():
    try:
        # Get parameters
        tier = request.args.get('tier', 'all')
        category = request.args.get('category', 'all')
        search = request.args.get('search', '').strip()
        search_mode = request.args.get('mode', 'all')
        sort_by = request.args.get('sort', 'subscribers')
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)
        nsfw_only = request.args.get('nsfw_only', '').lower() == 'true'

        conn = get_db_connection()
        cursor = conn.cursor()
        
        base_query = "FROM communities WHERE 1=1"
        count_query = f"SELECT COUNT(*) {base_query}"
        params = []
        
        if category != 'all':
            base_query += " AND category = ?"
            params.append(category)
        
        if tier == 'major':
            tier_params = [1000000]
            tier_clause = "subscribers >= ?"
        elif tier == 'rising':
            tier_params = [100000, 999999]
            tier_clause = "subscribers >= ? AND subscribers < ?"
        elif tier == 'growing':
            tier_params = [10000, 99999]
            tier_clause = "subscribers >= ? AND subscribers < ?"
        elif tier == 'emerging':
            tier_params = [1000, 9999]
            tier_clause = "subscribers >= ? AND subscribers < ?"
        else:
            tier_params = []
            tier_clause = ""
        
        params.extend(tier_params)
        if tier_clause:
            base_query += f" AND {tier_clause}"
        
        search_uses_fts = False
        if search:
            if search_mode == 'name':
                base_query += f" AND LOWER(display_name) LIKE ?"
                params.append(f"%{search.lower()}%")
                count_query = f"SELECT COUNT(*) {base_query}"
            elif search_mode == 'description':
                base_query += " AND LOWER(public_description) LIKE ?"
                params.append(f"%{search.lower()}%")
                count_query = f"SELECT COUNT(*) {base_query}"
            else:
                search_uses_fts = True
                base_query = (
                    "FROM communities c "
                    "INNER JOIN communities_fts ON c.id = communities_fts.rowid "
                    f"WHERE communities_fts MATCH ?"
                )
                if tier != 'all' or category != 'all':
                    if tier_clause:
                        base_query += f" AND {tier_clause}"
                    if category != 'all':
                        base_query += " AND c.category = ?"
                        params.insert(0, category)
                    params.insert(0, search)
                    count_query = f"SELECT COUNT(*) {base_query}"
                else:
                    params = [search]
                    count_query = f"SELECT COUNT(*) {base_query}"
        
        if nsfw_only:
            base_query += " AND over18 = 1"

        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        
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
        
        offset = (page - 1) * per_page
        select_query = f"SELECT * {base_query} {sort_clause} LIMIT ? OFFSET ?"
        query_params = params + [per_page, offset]
        cursor.execute(select_query, query_params)
        rows = cursor.fetchall()
        
        result_data = []
        for row in rows:
            item = {
                'display_name': row['display_name'] or '',
                'url': row['url'] or '',
                'subscribers': int(row['subscribers'] or 0),
                'created_date': row['created_date'] or '',
                'public_description': row['public_description'] or '',
                'description': row['description'] or '',
                'over18': int(row['over18'] or 0),
                'subreddit_type': row['subreddit_type'] or '',
                'name': row['name'] or '',
                'title': row['title'] or '',
                'category': row['category'] or ''
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
            params.extend([100000, 999999])
        elif tier == 'growing':
            base_query += " AND subscribers >= ? AND subscribers < ?"
            params.extend([10000, 99999])
        elif tier == 'emerging':
            base_query += " AND subscribers >= ? AND subscribers < ?"
            params.extend([1000, 9999])
        
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

@app.route('/api/available_years')
def get_available_years():
    try:
        subreddit = request.args.get('subreddit', '')
        cleaned_subreddit = subreddit.rstrip('/')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if subreddit:
            cursor.execute("""
                SELECT DISTINCT year 
                FROM comment_history 
                WHERE subreddit = ? 
                ORDER BY year DESC
            """, [cleaned_subreddit])
        else:
            cursor.execute("""
                SELECT DISTINCT year 
                FROM comment_history 
                ORDER BY year DESC
            """)
        
        years = [row['year'] for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({'years': years})
        
    except Exception as e:
        print(f"Error in /api/available_years: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route("/api/comments/<subreddit>")
def get_monthly_comments(subreddit):
    import sqlite3
    conn = sqlite3.connect("reddit_communities.db")
    cursor = conn.cursor()
    tables = [t[0] for t in cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'comment_count_%'")]
    data = []
    for table in sorted(tables):
        cursor.execute(f"SELECT subreddit, month_comment_count FROM {table} WHERE subreddit=?", (subreddit,))
        row = cursor.fetchone()
        if row:
            month = table.split("_")[-2] + "-" + table.split("_")[-1]  # e.g., 2009-04
            data.append({"month": month, "count": row[1]})
    conn.close()
    return {"data": data}


@app.route('/api/month_data')
def get_month_data():
    try:
        subreddit = request.args.get('subreddit', '')
        year = request.args.get('year', '')
        month = request.args.get('month', '')
        
        cleaned_subreddit = subreddit.rstrip('/')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT day, SUM(comment_count) as total_comments
            FROM comment_history 
            WHERE subreddit = ? AND year = ? AND month = ?
            GROUP BY day
            ORDER BY day
        """, [cleaned_subreddit, year, month])
        
        days = [{'day': row['day'], 'comment_count': row['total_comments']} 
               for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({'days': days})
        
    except Exception as e:
        print(f"Error in /api/month_data: {str(e)}")
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


@app.route('/api/debug_year_data')
def debug_year_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if we have any year data
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM comment_history 
            WHERE period_type = 'year'
        """)
        year_count = cursor.fetchone()['count']
        
        # Get some sample year data
        cursor.execute("""
            SELECT subreddit, period_value, comment_count
            FROM comment_history 
            WHERE period_type = 'year'
            LIMIT 10
        """)
        sample_year_data = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'year_data_count': year_count,
            'sample_year_data': sample_year_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug_period_types')
def debug_period_types():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check what period types exist
        cursor.execute("SELECT DISTINCT period_type FROM comment_history")
        period_types = [row['period_type'] for row in cursor.fetchall()]
        
        # Check some sample data
        cursor.execute("SELECT * FROM comment_history LIMIT 5")
        sample_data = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'period_types': period_types,
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
    
    app.run(debug=True, port=5001, threaded=True)
