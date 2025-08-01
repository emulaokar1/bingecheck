#!/usr/bin/env python3
"""
Detailed debug of Game of Thrones posts to understand the content
"""

import pandas as pd
import sys
from pathlib import Path

def analyze_got_posts_detailed():
    """Detailed analysis of Game of Thrones posts"""
    
    # Load Reddit discussions
    reddit_file = Path("data/processed/reddit_discussions.csv")
    discussions_df = pd.read_csv(reddit_file)
    
    # Filter for Game of Thrones (show_id = 6)
    got_posts = discussions_df[discussions_df['show_id'] == 6].copy()
    
    print(f"🎬 Game of Thrones Analysis:")
    print(f"Total posts: {len(got_posts)}")
    
    # Analyze content distribution
    posts_with_content = got_posts[got_posts['content'].notna() & (got_posts['content'].astype(str).str.len() > 0)]
    print(f"Posts with content: {len(posts_with_content)}")
    print(f"Posts without content: {len(got_posts) - len(posts_with_content)}")
    
    print("\n📝 Posts with actual content:")
    for i, (_, post) in enumerate(posts_with_content.head(10).iterrows(), 1):
        title = post['title']
        content = str(post['content'])
        score = post['score']
        
        print(f"\n{i}. [{score} score] {title}")
        print(f"   Content: {content[:150]}{'...' if len(content) > 150 else ''}")
        
        # Check for our keywords
        when_good_keywords = [
            "gets good", "worth watching", "should I continue", "stick with it", 
            "slow start", "boring at first", "gets better after", "improves", 
            "first season slow", "when does it pick up", "worth pushing through",
            "skip first season", "starts slow", "hard to get into", "give it time",
            "patient with", "rough start", "first episodes boring", "when to stop watching",
            "does it get better", "struggle through", "payoff", "investment"
        ]
        
        combined_content = (title + ' ' + content).lower()
        matched_keywords = [kw for kw in when_good_keywords if kw.lower() in combined_content]
        
        if matched_keywords:
            print(f"   🎯 MATCHED KEYWORDS: {matched_keywords}")
    
    # Now check what type of discussions we have
    print(f"\n📊 Types of posts:")
    print(f"Image/Video posts (no content): {len(got_posts[got_posts['content'].isna() | (got_posts['content'].astype(str).str.len() == 0)])}")
    print(f"Discussion posts (with content): {len(posts_with_content)}")
    
    # Check subreddits
    print(f"\n📍 Subreddits:")
    subreddit_counts = got_posts['subreddit'].value_counts()
    for subreddit, count in subreddit_counts.head(5).items():
        print(f"  {subreddit}: {count} posts")
    
    # Check is_discussion flag
    if 'is_discussion' in got_posts.columns:
        discussion_posts = got_posts[got_posts['is_discussion'] == True]
        print(f"\n💬 Posts marked as discussions: {len(discussion_posts)}")
        
        if len(discussion_posts) > 0:
            print("Sample discussion posts:")
            for i, (_, post) in enumerate(discussion_posts.head(3).iterrows(), 1):
                print(f"{i}. {post['title']} (Score: {post['score']})")
                if pd.notna(post['content']) and len(str(post['content'])) > 0:
                    print(f"   Content: {str(post['content'])[:100]}...")

if __name__ == "__main__":
    analyze_got_posts_detailed()