from flask import Blueprint, jsonify
from utils.db import get_db_connection
from pathlib import Path

debug_bp = Blueprint('debug', __name__)

@debug_bp.route('/debug')
def debug_info():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM communities LIMIT 1")
        row = cursor.fetchone()
        sample_data = dict(row) if row else None
        
        cursor.execute("PRAGMA table_info(communities)")
        table_info = cursor.fetchall()
        
        db_size = Path('../../reddit_communities.db').stat().st_size / (1024*1024) if Path('../../reddit_communities.db').exists() else 0
        
        conn.close()
        
        return jsonify({
            'database_path': '../../reddit_communities.db',
            'database_size_mb': round(db_size, 2),
            'table_info': table_info,
            'sample_data': sample_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@debug_bp.route('/debug_year_data')
def debug_year_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM comment_history 
            WHERE period_type = 'year'
        """)
        year_count = cursor.fetchone()['count']
        
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

@debug_bp.route('/debug_period_types')
def debug_period_types():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT period_type FROM comment_history")
        period_types = [row['period_type'] for row in cursor.fetchall()]
        
        cursor.execute("SELECT * FROM comment_history LIMIT 5")
        sample_data = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'period_types': period_types,
            'sample_data': sample_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
