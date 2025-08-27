# scripts/check_subreddit_sources.py
import pandas as pd
import json

# Load Reddit discussions
reddit_df = pd.read_csv('data/processed/reddit_discussions.csv')

# Load show info
with open('data/analysis/llm_analysis_results.json', 'r') as f:
    llm_data = json.load(f)
    show_mapping = {item['show_id']: item['show_title'] for item in llm_data}

# Shows to investigate
problem_shows = {
    50: "The Lord of the Rings: The Rings of Power",
    53: "Avatar: The Last Airbender"
}

print("SUBREDDIT ANALYSIS FOR PROBLEM SHOWS")
print("=" * 80)

for show_id, title in problem_shows.items():
    print(f"\n📺 {title} (ID: {show_id})")
    print("-" * 80)
    
    # Get all posts for this show
    show_posts = reddit_df[reddit_df['show_id'] == show_id]
    
    if len(show_posts) == 0:
        print("❌ No Reddit posts found!")
        continue
    
    # Analyze subreddits
    subreddit_counts = show_posts['subreddit'].value_counts()
    
    print(f"Total posts: {len(show_posts)}")
    print(f"\nSubreddits scraped:")
    for sub, count in subreddit_counts.items():
        print(f"  r/{sub}: {count} posts")
    
    # Show sample post titles
    print(f"\nSample post titles:")
    for _, post in show_posts.head(5).iterrows():
        print(f"  - {post['title'][:80]}...")

# Check overall subreddit distribution
print("\n\n" + "=" * 80)
print("OVERALL SUBREDDIT DISTRIBUTION")
print("=" * 80)

all_subreddits = reddit_df['subreddit'].value_counts()
print(f"\nTop 20 subreddits by post count:")
for sub, count in all_subreddits.head(20).items():
    print(f"  r/{sub}: {count} posts")

# Check if specific subreddits were missed
expected_subs = ['LOTR_on_Prime', 'TheLastAirbender', 'RingsofPower', 'ATLA']
print(f"\n\nChecking for expected subreddits:")
for sub in expected_subs:
    count = all_subreddits.get(sub, 0)
    status = "✅" if count > 0 else "❌"
    print(f"  {status} r/{sub}: {count} posts")