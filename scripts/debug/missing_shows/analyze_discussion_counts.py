# scripts/analyze_discussion_counts.py
import pandas as pd
import json

# Load Reddit discussions
reddit_df = pd.read_csv('data/processed/reddit_discussions.csv')

# Load show titles
with open('data/analysis/llm_analysis_results.json', 'r') as f:
    llm_data = json.load(f)
    show_id_to_title = {item['show_id']: item['show_title'] for item in llm_data}

# Count discussions per show
discussion_counts = reddit_df['show_id'].value_counts().to_dict()

# Create summary with titles
show_summaries = []
for show_id, title in show_id_to_title.items():
    count = discussion_counts.get(show_id, 0)
    show_summaries.append({
        'show_id': show_id,
        'title': title,
        'discussion_count': count
    })

# Sort by discussion count
show_summaries.sort(key=lambda x: x['discussion_count'])

# Display results
print("Reddit Discussion Counts by Show (BEFORE filtering)")
print("=" * 60)
print(f"{'Show Title':<35} {'Show ID':<10} {'Discussions':<15}")
print("-" * 60)

# Shows with fewest discussions
print("\n🔴 SHOWS WITH FEW/NO DISCUSSIONS:")
for show in show_summaries[:15]:
    print(f"{show['title']:<35} {show['show_id']:<10} {show['discussion_count']:<15}")

# Shows with most discussions  
print("\n🟢 SHOWS WITH MOST DISCUSSIONS:")
for show in show_summaries[-10:]:
    print(f"{show['title']:<35} {show['show_id']:<10} {show['discussion_count']:<15}")

# Summary statistics
print("\n" + "=" * 60)
print("SUMMARY STATISTICS:")
print("-" * 60)
total_shows = len(show_summaries)
shows_with_zero = sum(1 for s in show_summaries if s['discussion_count'] == 0)
shows_under_10 = sum(1 for s in show_summaries if s['discussion_count'] < 10)
shows_under_50 = sum(1 for s in show_summaries if s['discussion_count'] < 50)

print(f"Total shows analyzed: {total_shows}")
print(f"Shows with 0 Reddit discussions: {shows_with_zero}")
print(f"Shows with <10 discussions: {shows_under_10}")
print(f"Shows with <50 discussions: {shows_under_50}")
print(f"\nAverage discussions per show: {reddit_df['show_id'].value_counts().mean():.1f}")
print(f"Median discussions per show: {reddit_df['show_id'].value_counts().median():.1f}")

# Distribution breakdown
print("\nDISCUSSION COUNT DISTRIBUTION:")
print("-" * 60)
ranges = [(0, 0), (1, 10), (11, 50), (51, 100), (101, 500), (501, float('inf'))]
for low, high in ranges:
    if high == float('inf'):
        count = sum(1 for s in show_summaries if s['discussion_count'] > low)
        print(f"{low}+ discussions: {count} shows")
    elif low == high:
        count = sum(1 for s in show_summaries if s['discussion_count'] == low)
        print(f"Exactly {low} discussions: {count} shows")
    else:
        count = sum(1 for s in show_summaries if low <= s['discussion_count'] <= high)
        print(f"{low}-{high} discussions: {count} shows")