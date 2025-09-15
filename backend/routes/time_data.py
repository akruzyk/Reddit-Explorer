from flask import Blueprint, jsonify, request
from utils.db import get_db_connection

time_data_bp = Blueprint('time_data', __name__)

@time_data_bp.route('/api/subscriber-history/<subreddit>')
def get_subscriber_history(subreddit):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Query to get monthly subscriber counts for a specific subreddit
        cursor.execute("""
            SELECT year, month, subscribers 
            FROM subscriber_history 
            WHERE subreddit_name = ? 
            ORDER BY year, month
        """, (subreddit,))
        
        data = cursor.fetchall()
        
        # Format the data for the frontend
        history_data = [
            {
                'date': f"{row['year']}-{row['month']:02d}",
                'subscribers': row['subscribers']
            }
            for row in data
        ]
        
        return jsonify(history_data)
    
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@time_data_bp.route('/available_years')
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

@time_data_bp.route('/month_data')
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
