#!/usr/bin/env python3
"""
Debug script to test the Game of Thrones "when gets good" filtering logic
"""

import pandas as pd
import sys
from pathlib import Path

def debug_got_filtering():
    """Debug the filtering logic for Game of Thrones"""
    
    # Load Reddit discussions
    reddit_file = Path("data/processed/reddit_discussions.csv")
    
    if not reddit_file.exists():
        print(f"❌ Reddit discussions file not found: {reddit_file}")
        return
        
    print("📁 Loading Reddit discussions...")
    discussions_df = pd.read_csv(reddit_file)
    print(f"✅ Loaded {len(discussions_df)} total discussions")
    
    # Filter for Game of Thrones (show_id = 6)
    got_posts = discussions_df[discussions_df['show_id'] == 6].copy()
    print(f"🎬 Found {len(got_posts)} Game of Thrones posts")
    
    # Keywords from the LLM analysis code
    when_good_keywords = [
        "gets good", "worth watching", "should I continue", "stick with it", 
        "slow start", "boring at first", "gets better after", "improves", 
        "first season slow", "when does it pick up", "worth pushing through",
        "skip first season", "starts slow", "hard to get into", "give it time",
        "patient with", "rough start", "first episodes boring", "when to stop watching",
        "does it get better", "struggle through", "payoff", "investment"
    ]
    
    print(f"🔍 Searching for posts with keywords: {when_good_keywords[:5]}...")
    
    # Apply the same filtering logic as in the LLM analysis
    filtered_posts = []
    posts_with_content = 0
    
    for _, post in got_posts.iterrows():
        # Handle NaN/float content safely (same as in llm_analysis.py)
        title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
        content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
        
        combined_content = title + ' ' + content
        content_lower = combined_content.lower()
        
        if len(content.strip()) > 0:  # Count posts with actual content
            posts_with_content += 1
        
        # Check if any keyword matches
        matched_keywords = []
        for keyword in when_good_keywords:
            if keyword.lower() in content_lower:
                matched_keywords.append(keyword)
        
        if matched_keywords:
            filtered_posts.append({
                'title': title,
                'content': content[:200] + "..." if len(content) > 200 else content,
                'score': post.get('score', 0),
                'matched_keywords': matched_keywords,
                'subreddit': str(post.get('subreddit', '')) if pd.notna(post.get('subreddit')) else '',
                'created_utc': str(post.get('created_utc', '')) if pd.notna(post.get('created_utc')) else ''
            })
    
    print(f"📊 Posts with actual content: {posts_with_content}")
    print(f"🎯 Posts matching 'when gets good' keywords: {len(filtered_posts)}")
    
    if filtered_posts:
        print("\n✅ Found matching posts:")
        for i, post in enumerate(filtered_posts[:3], 1):
            print(f"\n{i}. Title: {post['title']}")
            print(f"   Score: {post['score']}")
            print(f"   Keywords matched: {post['matched_keywords']}")
            print(f"   Content: {post['content']}")
    else:
        print("\n❌ No posts found matching 'when gets good' keywords")
        
        # Let's check what the actual content looks like
        print("\n🔍 Sample post titles and content:")
        sample_posts = got_posts.head(10)
        for _, post in sample_posts.iterrows():
            title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
            content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
            print(f"Title: {title}")
            print(f"Content length: {len(content)}")
            if len(content.strip()) > 0:
                print(f"Content snippet: {content[:100]}...")
            print("---")
    
    # Check for alternative keywords that might be present
    print("\n🔍 Checking for alternative keywords in titles and content...")
    alternative_keywords = ['first season', 'season 1', 'beginning', 'start', 'early episodes', 'pilot']
    
    for keyword in alternative_keywords:
        matching_count = 0
        for _, post in got_posts.iterrows():
            title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
            content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
            combined_content = (title + ' ' + content).lower()
            
            if keyword.lower() in combined_content:
                matching_count += 1
        
        if matching_count > 0:
            print(f"'{keyword}': {matching_count} posts")

if __name__ == "__main__":
    debug_got_filtering()