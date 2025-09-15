from flask import Blueprint, jsonify
from utils.db import get_db_connection, check_database

health_bp = Blueprint('health', __name__)

@health_bp.route('/health')
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
        
        cursor.execute("SELECT COUNT(*) FROM communities")
        total = cursor.fetchone()[0]
        
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
