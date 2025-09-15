#!/usr/bin/env python3
"""
Script to merge comment counts with subreddit metadata files
and generate consolidated output files
"""

import csv
import os
import glob
from collections import defaultdict
import argparse

def load_comment_counts(comment_count_file):
    """
    Load comment counts from the CSV file
    
    Args:
        comment_count_file (str): Path to the comment count CSV file
        
    Returns:
        dict: Dictionary mapping subreddit names to comment counts
    """
    comment_counts = {}
    
    if not os.path.exists(comment_count_file):
        print(f"❌ Error: Comment count file '{comment_count_file}' not found.")
        return None
    
    print(f"📊 Loading comment counts from: {comment_count_file}")
    
    try:
        with open(comment_count_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                subreddit = row['subreddit'].strip().lower()
                comment_counts[subreddit] = int(row['comment_count'])
        
        print(f"✅ Loaded comment counts for {len(comment_counts):,} subreddits")
        return comment_counts
        
    except Exception as e:
        print(f"❌ Error loading comment counts: {e}")
        return None

def find_subreddit_files(folder_path):
    """
    Find all subreddit CSV files in the specified folder
    
    Args:
        folder_path (str): Path to the folder containing subreddit CSV files
        
    Returns:
        list: List of CSV file paths
    """
    if not os.path.exists(folder_path):
        print(f"❌ Error: Folder '{folder_path}' not found.")
        return []
    
    # Look for CSV files with the expected naming pattern
    csv_files = glob.glob(os.path.join(folder_path, "*_over18_*.csv"))
    
    if not csv_files:
        print(f"❌ No CSV files found in '{folder_path}'")
        print("💡 Looking for files with pattern: '*_over18_*.csv'")
        # Try a broader search
        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    print(f"📁 Found {len(csv_files)} CSV files in '{folder_path}'")
    for file in csv_files:
        print(f"   - {os.path.basename(file)}")
    
    return csv_files

def process_subreddit_file(input_file, comment_counts, output_folder):
    """
    Process a single subreddit CSV file and add comment counts
    
    Args:
        input_file (str): Path to the input CSV file
        comment_counts (dict): Dictionary of comment counts by subreddit
        output_folder (str): Folder to save output files
        
    Returns:
        list: List of rows with added comment_count field
    """
    print(f"🔍 Processing: {os.path.basename(input_file)}")
    
    rows_with_comments = []
    matched_count = 0
    unmatched_count = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            csv_reader = csv.DictReader(infile)
            fieldnames = csv_reader.fieldnames
            
            # Add comment_count to fieldnames if not already present
            if 'comment_count' not in fieldnames:
                fieldnames.append('comment_count')
            
            for row in csv_reader:
                subreddit_name = row.get('display_name', '').strip().lower()
                if not subreddit_name:
                    unmatched_count += 1
                    row['comment_count'] = '0'
                    rows_with_comments.append(row)
                    continue
                
                # Try to find matching comment count
                comment_count = comment_counts.get(subreddit_name, 0)
                
                if comment_count > 0:
                    matched_count += 1
                else:
                    unmatched_count += 1
                
                row['comment_count'] = str(comment_count)
                rows_with_comments.append(row)
        
        print(f"   ✅ Matched comment counts for {matched_count:,} subreddits")
        print(f"   ❌ No comment data for {unmatched_count:,} subreddits")
        
        # Generate output filename
        filename = os.path.basename(input_file)
        name_parts = os.path.splitext(filename)[0].split('_')
        
        # Create new filename with comment count
        if 'over18' in name_parts:
            over18_index = name_parts.index('over18')
            name_parts.insert(over18_index + 2, 'with_comments')
        else:
            name_parts.append('with_comments')
        
        output_filename = '_'.join(name_parts) + '.csv'
        output_path = os.path.join(output_folder, output_filename)
        
        # Write the enhanced CSV file
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            csv_writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(rows_with_comments)
        
        print(f"   💾 Saved enhanced file: {output_filename}")
        
        return rows_with_comments
        
    except Exception as e:
        print(f"❌ Error processing file {input_file}: {e}")
        return []

def create_consolidated_file(all_rows, output_folder, comment_counts):
    """
    Create a consolidated CSV file with all subreddits
    
    Args:
        all_rows (list): List of all rows from all files
        output_folder (str): Folder to save the consolidated file
        comment_counts (dict): Dictionary of comment counts
    """
    if not all_rows:
        print("❌ No data to consolidate")
        return
    
    print("🔗 Creating consolidated CSV file...")
    
    # Get fieldnames from the first row
    fieldnames = list(all_rows[0].keys()) if all_rows else []
    
    # Ensure comment_count is in fieldnames
    if 'comment_count' not in fieldnames:
        fieldnames.append('comment_count')
    
    output_path = os.path.join(output_folder, 'all_subreddits_with_comments.csv')
    
    try:
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            csv_writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(all_rows)
        
        # Calculate some statistics
        total_subreddits = len(all_rows)
        subreddits_with_comments = sum(1 for row in all_rows if int(row.get('comment_count', 0)) > 0)
        
        print(f"✅ Consolidated file created: {os.path.basename(output_path)}")
        print(f"📊 Total subreddits: {total_subreddits:,}")
        print(f"📊 Subreddits with comment data: {subreddits_with_comments:,} ({subreddits_with_comments/total_subreddits*100:.1f}%)")
        print(f"📊 Subreddits without comment data: {total_subreddits - subreddits_with_comments:,} ({(total_subreddits - subreddits_with_comments)/total_subreddits*100:.1f}%)")
        
    except Exception as e:
        print(f"❌ Error creating consolidated file: {e}")

def main():
    parser = argparse.ArgumentParser(description='Merge comment counts with subreddit metadata files')
    parser.add_argument('comment_count_file', help='Path to the comment count CSV file')
    parser.add_argument('subreddit_folder', help='Path to the folder containing subreddit CSV files')
    parser.add_argument('--output-folder', default='./output', help='Path to the output folder (default: ./output)')
    
    args = parser.parse_args()
    
    # Create output folder if it doesn't exist
    if not os.path.exists(args.output_folder):
        os.makedirs(args.output_folder)
        print(f"📁 Created output folder: {args.output_folder}")
    
    # Load comment counts
    comment_counts = load_comment_counts(args.comment_count_file)
    if not comment_counts:
        return
    
    # Find subreddit files
    subreddit_files = find_subreddit_files(args.subreddit_folder)
    if not subreddit_files:
        return
    
    # Process each file
    all_rows = []
    for file_path in subreddit_files:
        rows = process_subreddit_file(file_path, comment_counts, args.output_folder)
        all_rows.extend(rows)
    
    # Create consolidated file
    create_consolidated_file(all_rows, args.output_folder, comment_counts)
    
    print("=" * 60)
    print("✅ All processing complete!")
    print(f"📁 Output files saved to: {args.output_folder}")

if __name__ == "__main__":
    main()