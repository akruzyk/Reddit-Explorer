from flask import Blueprint, jsonify, request
from utils.db import get_db_connection
import traceback

communities_bp = Blueprint('communities', __name__)

@communities_bp.route('/communities')
def get_communities():
    try:
        tier = request.args.get('tier', 'all')
        category = request.args.get('category', 'all')
        search = request.args.get('search', '').strip()
        search_mode = request.args.get('mode', 'all')
        sort_by = request.args.get('sort', 'subscribers')
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)
        nsfw_only = request.args.get('nsfw_only', 'false').lower() == 'true'

        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Initialize queries and parameters
        base_query = "FROM communities"
        count_query_base = "SELECT COUNT(*) FROM communities"
        params = []
        count_params = []
        
        conditions = []
        if category == 'nsfw':
            conditions.append("over18 = ?")
            params.append(1)
        elif category != 'all':
            conditions.append("category = ?")
            params.append(category)
        
        if tier == 'major':
            conditions.append("subscribers >= ?")
            params.append(1000000)
        elif tier == 'rising':
            conditions.append("subscribers BETWEEN ? AND ?")
            params.extend([100000, 999999])
        elif tier == 'growing':
            conditions.append("subscribers BETWEEN ? AND ?")
            params.extend([10000, 99999])
        elif tier == 'emerging':
            conditions.append("subscribers BETWEEN ? AND ?")
            params.extend([1000, 9999])
        
        # Handle search with FTS
        using_fts = False
        if search:
            if search_mode == 'name':
                conditions.append("LOWER(display_name) LIKE ?")
                params.append(f"%{search.lower()}%")
            elif search_mode == 'description':
                conditions.append("LOWER(public_description) LIKE ?")
                params.append(f"%{search.lower()}%")
            else:  # 'all' mode - use FTS
                using_fts = True
                base_query = (
                    "FROM communities c "
                    "INNER JOIN communities_fts ON c.id = communities_fts.rowid "
                    "WHERE communities_fts MATCH ?"
                )
                count_query_base = (
                    "SELECT COUNT(*) FROM communities c "
                    "INNER JOIN communities_fts ON c.id = communities_fts.rowid "
                    "WHERE communities_fts MATCH ?"
                )
                fts_search_term = search if search.isalnum() else f'"{search}"'
                params = [fts_search_term]
                count_params = [fts_search_term]
        
        # Build WHERE clause for non-FTS queries
        if not using_fts and conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query_base += where_clause
        
        # Handle NSFW filter
        if nsfw_only and not (category == 'nsfw' or any("over18" in cond for cond in conditions)):
            nsfw_condition = "over18 = ?"
            if "WHERE" in base_query:
                base_query += " AND " + nsfw_condition
                count_query_base += " AND " + nsfw_condition
            else:
                base_query += " WHERE " + nsfw_condition
                count_query_base += " WHERE " + nsfw_condition
            params.append(1)
        
        # Use count_params for FTS, otherwise use params
        final_count_params = count_params if using_fts else params
        
        # Execute count query
        cursor.execute(count_query_base, final_count_params)
        total = cursor.fetchone()[0]
        
        # Build sort clause
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
        
        # Execute main query
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


@communities_bp.route('/stats')
def get_stats():
    try:
        tier = request.args.get('tier', 'all')
        nsfw_only = request.args.get('nsfw_only', 'false').lower() == 'true'
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        base_query = "SELECT COUNT(*), SUM(subscribers), AVG(subscribers) FROM communities"
        params = []
        
        if nsfw_only:
            base_query += " WHERE over18 = ?"
            params.append(1)
        
        if tier == 'major':
            base_query += " WHERE " if not base_query.endswith("WHERE") else " AND "
            base_query += "subscribers >= ?"
            params.append(1000000)
        elif tier == 'rising':
            base_query += " WHERE " if not base_query.endswith("WHERE") else " AND "
            base_query += "subscribers >= ? AND subscribers < ?"
            params.extend([100000, 9999999])
        elif tier == 'growing':
            base_query += " WHERE " if not base_query.endswith("WHERE") else " AND "
            base_query += "subscribers >= ? AND subscribers < ?"
            params.extend([10000, 999999])
        elif tier == 'emerging':
            base_query += " WHERE " if not base_query.endswith("WHERE") else " AND "
            base_query += "subscribers >= ? AND subscribers < ?"
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
    

@communities_bp.route('/api/categories', methods=['GET'])
def get_categories():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM communities WHERE over18 = 1")
    nsfw_count = cursor.fetchone()[0]
    # Add other categories (e.g., from category column)
    cursor.execute("SELECT category, COUNT(*) FROM communities GROUP BY category")
    categories = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall() if row[0]]
    # Ensure NSFW is included
    if not any(c['name'] == 'NSFW' for c in categories):
        categories.append({'name': 'NSFW', 'count': nsfw_count})
    conn.close()
    return {'categories': categories}

@communities_bp.route('/api/comments/<subreddit>')
def get_subreddit_comments(subreddit):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all comment count tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'comment_count_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        monthly_data = []
        
        for table in sorted(tables):
            # Extract year and month from table name
            try:
                parts = table.split('_')
                year = int(parts[2])
                month = int(parts[3])
                
                # Query comment count for this subreddit in this month
                cursor.execute(f"SELECT month_comment_count FROM {table} WHERE subreddit = ?", (subreddit.lower(),))
                row = cursor.fetchone()
                
                if row and row[0] is not None:
                    monthly_data.append({
                        'month': f"{year}-{month:02d}",
                        'count': row[0],
                        'year': year,
                        'month_num': month
                    })
                    
            except (ValueError, IndexError):
                # Skip tables with malformed names
                continue
        
        conn.close()
        
        # Sort by date (oldest first)
        monthly_data.sort(key=lambda x: (x['year'], x['month_num']))
        
        return jsonify(monthly_data)
        
    except Exception as e:
        print(f"Error in /api/comments/{subreddit}: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': f'Server error: {str(e)}'}), 500