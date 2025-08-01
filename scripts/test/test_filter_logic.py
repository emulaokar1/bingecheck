#!/usr/bin/env python3
"""
Test the filter logic from LLM analysis to see which contaminated posts get through
"""

import pandas as pd

def test_filter_logic():
    # Load reddit discussions
    discussions_df = pd.read_csv("data/processed/reddit_discussions.csv")
    
    # Get the contaminated posts (show_id 6 posts mentioning "The 100")
    show_6_posts = discussions_df[discussions_df['show_id'] == 6].copy()
    the_100_posts = show_6_posts[
        show_6_posts['title'].str.contains('The 100', case=False, na=False) |
        show_6_posts['content'].str.contains('The 100', case=False, na=False)
    ]
    
    print(f"Found {len(the_100_posts)} contaminated posts")
    
    # Test the "when gets good" keywords from the LLM analysis
    when_good_keywords = [
        "gets good", "worth watching", "should I continue", "stick with it", 
        "slow start", "boring at first", "gets better after", "improves", 
        "first season slow", "when does it pick up", "worth pushing through",
        "skip first season", "starts slow", "hard to get into", "give it time",
        "patient with", "rough start", "first episodes boring", "when to stop watching",
        "does it get better", "struggle through", "payoff", "investment"
    ]
    
    print("\n=== TESTING WHEN GETS GOOD FILTER ===")
    
    filtered_posts = []
    
    for _, post in the_100_posts.iterrows():
        # Handle NaN/float content safely
        title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
        content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
        
        combined_content = title + ' ' + content
        content_lower = combined_content.lower()
        
        # Check if any keyword matches
        matching_keywords = []
        for keyword in when_good_keywords:
            if keyword.lower() in content_lower:
                matching_keywords.append(keyword)
        
        if matching_keywords:
            filtered_posts.append({
                'title': title,
                'content': content[:300],
                'reddit_id': post['reddit_id'],
                'matching_keywords': matching_keywords
            })
    
    print(f"Posts that would pass the 'when gets good' filter: {len(filtered_posts)}")
    
    for post in filtered_posts:
        print(f"\n--- Post {post['reddit_id']} ---")
        print(f"Title: {post['title']}")
        print(f"Content: {post['content']}...")
        print(f"Matching keywords: {post['matching_keywords']}")
        print("-" * 50)
    
    if len(filtered_posts) > 0:
        print(f"\n🚨 FOUND THE CULPRIT!")
        print(f"These {len(filtered_posts)} posts about 'The 100' contain keywords that")
        print(f"make them pass the 'when gets good' filter for Game of Thrones.")
        print(f"They get sent to the LLM, which then generates analysis about The 100")
        print(f"instead of Game of Thrones!")
    
if __name__ == "__main__":
    test_filter_logic()