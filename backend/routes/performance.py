from flask import Blueprint, jsonify, request
from utils.db import get_db_connection
import time

performance_bp = Blueprint('performance', __name__)

@performance_bp.route('/search-performance')
def search_performance():
    try:
        search_term = request.args.get('term', 'technology')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        results = {}
        
        start = time.time()
        cursor.execute("SELECT COUNT(*) FROM communities WHERE display_name LIKE ?", [f'%{search_term}%'])
        count = cursor.fetchone()[0]
        results['like_name'] = {
            'count': count,
            'time_ms': round((time.time() - start) * 1000, 2)
        }
        
        start = time.time()
        cursor.execute("SELECT COUNT(*) FROM communities WHERE public_description LIKE ?", 
                      [f'%{search_term}%'])
        count = cursor.fetchone()[0]
        results['like_description'] = {
            'count': count,
            'time_ms': round((time.time() - start) * 1000, 2)
        }
        
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
