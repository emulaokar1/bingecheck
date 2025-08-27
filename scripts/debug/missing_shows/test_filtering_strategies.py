# scripts/test_filtering_strategies.py
import pandas as pd
import json
from collections import defaultdict

# Load Reddit discussions
reddit_df = pd.read_csv('data/processed/reddit_discussions.csv')

# Load show mapping to get titles
with open('data/analysis/llm_analysis_results.json', 'r') as f:
    llm_data = json.load(f)
    show_id_to_title = {item['show_id']: item['show_title'] for item in llm_data}

def test_filtering_strategies(reddit_df, show_id_to_title):
    """Test different filtering strategies"""
    
    # Current strict keywords
    strict_keywords = [
        "gets good", "worth watching", "should I continue", "stick with it", 
        "slow start", "boring at first", "gets better after", "improves", 
        "first season slow", "when does it pick up", "finale", "ending", 
        "conclusion", "final episode", "series finale", "last season", 
        "disappointing", "satisfying", "best season", "peak", "masterpiece", 
        "best episode", "favorite episode", "amazing episode"
    ]
    
    # Broader keywords
    broad_keywords = strict_keywords + [
        "worth it", "recommend", "should I watch", "is it good",
        "keep watching", "give up", "skip", "favorite", "love this show",
        "hate this show", "overrated", "underrated", "episode discussion",
        "season review", "thoughts on", "just finished", "finally watched",
        "rewatch", "first time watching", "unpopular opinion"
    ]
    
    # Minimal keywords (just show discussion indicators)
    minimal_keywords = [
        "episode", "season", "finale", "worth", "good", "bad", 
        "watching", "finished", "started"
    ]
    
    results = defaultdict(dict)
    
    for show_id, title in show_id_to_title.items():
        show_posts = reddit_df[reddit_df['show_id'] == show_id]
        total_posts = len(show_posts)
        
        # Test strict filtering
        strict_count = 0
        for _, post in show_posts.iterrows():
            text = f"{post.get('title', '')} {post.get('content', '')}".lower()
            if any(keyword in text for keyword in strict_keywords):
                strict_count += 1
        
        # Test broad filtering
        broad_count = 0
        for _, post in show_posts.iterrows():
            text = f"{post.get('title', '')} {post.get('content', '')}".lower()
            if any(keyword in text for keyword in broad_keywords):
                broad_count += 1
        
        # Test minimal filtering
        minimal_count = 0
        for _, post in show_posts.iterrows():
            text = f"{post.get('title', '')} {post.get('content', '')}".lower()
            if any(keyword in text for keyword in minimal_keywords):
                minimal_count += 1
        
        # Test score-based filtering (no keywords, just high-quality posts)
        high_score_posts = show_posts[show_posts['score'] > 50]
        score_based_count = len(high_score_posts)
        
        results[show_id] = {
            'title': title,
            'total_posts': total_posts,
            'strict_keywords': strict_count,
            'broad_keywords': broad_count,
            'minimal_keywords': minimal_count,
            'score_based': score_based_count
        }
    
    return results

# Run the test
results = test_filtering_strategies(reddit_df, show_id_to_title)

# Display results sorted by shows with few discussions
print("Shows with Few/No Discussions Under Different Strategies:")
print("=" * 100)
print(f"{'Show':<30} {'Total':<8} {'Strict':<8} {'Broad':<8} {'Minimal':<10} {'Score>50':<10}")
print("-" * 100)

# Sort by strict keyword count to see problematic shows first
sorted_results = sorted(results.items(), key=lambda x: x[1]['strict_keywords'])

for show_id, data in sorted_results[:20]:  # Show bottom 20
    print(f"{data['title']:<30} {data['total_posts']:<8} {data['strict_keywords']:<8} "
          f"{data['broad_keywords']:<8} {data['minimal_keywords']:<10} {data['score_based']:<10}")

# Summary statistics
print("\n" + "=" * 100)
print("SUMMARY STATISTICS:")
print("-" * 100)

total_shows = len(results)
zero_strict = sum(1 for r in results.values() if r['strict_keywords'] == 0)
zero_broad = sum(1 for r in results.values() if r['broad_keywords'] == 0)
zero_any = sum(1 for r in results.values() if r['total_posts'] == 0)

print(f"Shows with NO Reddit posts at all: {zero_any}")
print(f"Shows with 0 matches (strict keywords): {zero_strict}")
print(f"Shows with 0 matches (broad keywords): {zero_broad}")
print(f"\nAverage discussions per show:")
print(f"  Strict keywords: {sum(r['strict_keywords'] for r in results.values()) / total_shows:.1f}")
print(f"  Broad keywords: {sum(r['broad_keywords'] for r in results.values()) / total_shows:.1f}")
print(f"  Score-based (>50): {sum(r['score_based'] for r in results.values()) / total_shows:.1f}")

# Show keyword effectiveness
print("\n" + "=" * 100)
print("KEYWORD EFFECTIVENESS TEST:")
print("-" * 100)

# Test individual keyword effectiveness
keyword_hits = defaultdict(int)
for _, post in reddit_df.iterrows():
    text = f"{post.get('title', '')} {post.get('content', '')}".lower()
    for keyword in strict_keywords:
        if keyword in text:
            keyword_hits[keyword] += 1

print("Top 10 most effective keywords:")
for keyword, hits in sorted(keyword_hits.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  '{keyword}': {hits} hits")

print("\nLeast effective keywords:")
for keyword, hits in sorted(keyword_hits.items(), key=lambda x: x[1])[:10]:
    print(f"  '{keyword}': {hits} hits")