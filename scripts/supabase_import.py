import json
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import re

load_dotenv()

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(url, key)

# Load your LLM analysis
with open('data/analysis/llm_analysis_results.json', 'r') as f:
    llm_data = json.load(f)

# Load Reddit discussions to count them
with open('data/processed/reddit_discussions.csv', 'r') as f:
    import pandas as pd
    reddit_df = pd.read_csv(f)

# Function to count relevant discussions per show
def count_relevant_discussions(show_id, reddit_df):
    """Count discussions that would have been analyzed"""
    
    # Keywords used in your filtering
    all_keywords = [
        # when_gets_good keywords
        "gets good", "worth watching", "should I continue", "stick with it", 
        "slow start", "boring at first", "gets better after", "improves", 
        "first season slow", "when does it pick up",
        
        # ending keywords
        "finale", "ending", "conclusion", "final episode", "series finale", 
        "last season", "disappointing", "satisfying",
        
        # best content keywords
        "best season", "peak", "masterpiece", "best episode", "favorite episode", 
        "amazing episode"
    ]
    
    # Get posts for this show
    show_posts = reddit_df[reddit_df['show_id'] == show_id]
    
    if len(show_posts) == 0:
        return 0
    
    # Count posts that match any keyword
    relevant_count = 0
    for _, post in show_posts.iterrows():
        title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
        content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
        combined = (title + ' ' + content).lower()
        
        if any(keyword.lower() in combined for keyword in all_keywords):
            relevant_count += 1
    
    return relevant_count

# Process each show
successful_imports = 0
failed_imports = 0

for analysis in llm_data:
    show_id = analysis['show_id']
    
    try:
        # Count relevant discussions
        discussion_count = count_relevant_discussions(show_id, reddit_df)
        
        # Prepare data for insertion
        insert_data = {
            'show_id': show_id,
            'when_gets_good': analysis.get('when_gets_good'),
            'ending_sentiment': analysis.get('ending_sentiment'),
            'best_content_type': analysis.get('best_content', {}).get('type'),
            'best_content_description': analysis.get('best_content', {}).get('description'),
            'viewer_appeal': analysis.get('viewer_appeal'),
            'total_discussions': discussion_count,
            'last_calculated': 'now()'
        }
        
        # Remove None values to avoid issues
        insert_data = {k: v for k, v in insert_data.items() if v is not None}
        
        # Insert into database
        existing = supabase.table('show_statistics').select('show_id').eq('show_id', show_id).execute()

        if existing.data:
            # Update
            result = supabase.table('show_statistics').update(insert_data).eq('show_id', show_id).execute()
        else:
            # Insert
            result = supabase.table('show_statistics').insert(insert_data).execute()
        successful_imports += 1
        
    except Exception as e:
        print(f"✗ Failed: Show {show_id} - Error: {str(e)}")
        failed_imports += 1

print(f"\n{'='*50}")
print(f"Import Complete!")
print(f"Successful: {successful_imports}")
print(f"Failed: {failed_imports}")
print(f"{'='*50}")

# Test the view to make sure it's working
print("\nTesting show_summary view...")
try:
    test_result = supabase.table('show_summary').select("*").limit(3).execute()
    if test_result.data:
        print(f"✓ View is working! Found {len(test_result.data)} shows")
        print(f"Example: {test_result.data[0]['title']} - Gets good: {test_result.data[0].get('when_gets_good', 'N/A')[:50]}...")
    else:
        print("✗ View returned no data")
except Exception as e:
    print(f"✗ Error testing view: {e}")