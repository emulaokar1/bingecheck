#!/usr/bin/env python3
"""
Debug script to identify the root cause of The 100 / Game of Thrones cross-contamination
"""

import pandas as pd
import json
from pathlib import Path

def load_data():
    """Load the necessary data files"""
    # Load peak analysis to get show mappings
    with open("data/analysis/peak_analysis_results.json", 'r') as f:
        peak_analysis = json.load(f)
    
    # Load reddit discussions
    discussions_df = pd.read_csv("data/processed/reddit_discussions.csv")
    
    return peak_analysis, discussions_df

def analyze_show_6_posts(discussions_df):
    """Analyze all posts assigned to show_id 6"""
    print("=== ANALYZING SHOW_ID 6 POSTS ===\n")
    
    show_6_posts = discussions_df[discussions_df['show_id'] == 6].copy()
    print(f"Total posts with show_id 6: {len(show_6_posts)}")
    
    # Check for "The 100" mentions
    the_100_posts = show_6_posts[
        show_6_posts['title'].str.contains('The 100', case=False, na=False) |
        show_6_posts['content'].str.contains('The 100', case=False, na=False)
    ]
    
    print(f"Posts mentioning 'The 100': {len(the_100_posts)}")
    
    if len(the_100_posts) > 0:
        print("\n--- THE 100 POSTS INCORRECTLY ASSIGNED TO GAME OF THRONES ---")
        for idx, post in the_100_posts.iterrows():
            print(f"\nPost ID: {post['reddit_id']}")
            print(f"Title: {post['title']}")
            print(f"Subreddit: {post['subreddit']}")
            print(f"Content preview: {str(post['content'])[:200]}...")
            print("-" * 50)
    
    # Check for actual Game of Thrones posts
    got_posts = show_6_posts[
        show_6_posts['title'].str.contains('Game of Thrones|GOT', case=False, na=False) |
        show_6_posts['content'].str.contains('Game of Thrones|GOT', case=False, na=False) |
        (show_6_posts['subreddit'].str.lower() == 'gameofthrones')
    ]
    
    print(f"\nActual Game of Thrones posts: {len(got_posts)}")
    print(f"Percentage of correct posts: {len(got_posts)/len(show_6_posts)*100:.1f}%")

def check_show_mappings(peak_analysis):
    """Check what shows are in our mapping"""
    print("\n=== SHOW MAPPINGS ===\n")
    
    for show in peak_analysis[:10]:  # First 10 shows
        print(f"show_id {show['show_id']}: {show['show_title']}")
    
    # Look for any show with "100" in the title
    for show in peak_analysis:
        if "100" in show['show_title']:
            print(f"Found show with '100': {show['show_id']} - {show['show_title']}")

def main():
    peak_analysis, discussions_df = load_data()
    
    check_show_mappings(peak_analysis)
    analyze_show_6_posts(discussions_df)
    
    print("\n=== ROOT CAUSE ANALYSIS ===")
    print("The issue is that posts about 'The 100' TV show are being incorrectly")
    print("assigned show_id 6, which should be reserved for Game of Thrones.")
    print("This happens because:")
    print("1. The Reddit scraper searches generic subreddits for Game of Thrones")
    print("2. It finds posts that mention 'The 100' in titles/content") 
    print("3. The show detection logic incorrectly maps these to show_id 6")
    print("4. These contaminated posts get passed to the LLM analysis")
    print("5. The LLM sees posts about 'The 100' and generates analysis about it")
    print("   instead of Game of Thrones")

if __name__ == "__main__":
    main()