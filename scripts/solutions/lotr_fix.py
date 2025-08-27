import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.processing.llm_analysis import TVShowLLMAnalyzer
import pandas as pd
import json
from supabase import create_client
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
)

# Get Reddit posts for LOTR
reddit_result = supabase.table('reddit_posts')\
    .select('*')\
    .eq('show_id', 50)\
    .execute()

discussions_df = pd.DataFrame(reddit_result.data)

# Load original peak analysis
with open('data/analysis/peak_analysis_results.json', 'r') as f:
    peak_data = json.load(f)

# Find LOTR's analysis
show_analysis = next(show for show in peak_data if show['show_id'] == 50)

# Run analysis
analyzer = TVShowLLMAnalyzer()
llm_result = analyzer.analyze_single_show(show_analysis, discussions_df)

# Update database
if llm_result:
    # Count relevant discussions
    all_keywords = [
        "gets good", "worth watching", "should I continue", "stick with it", 
        "slow start", "boring at first", "gets better after", "improves", 
        "first season slow", "when does it pick up", "finale", "ending", 
        "conclusion", "final episode", "series finale", "last season", 
        "disappointing", "satisfying", "best season", "peak", "masterpiece", 
        "best episode", "favorite episode", "amazing episode"
    ]
    
    relevant_count = 0
    for _, post in discussions_df.iterrows():
        title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
        content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
        combined = (title + ' ' + content).lower()
        
        if any(keyword.lower() in combined for keyword in all_keywords):
            relevant_count += 1
    
    update_data = {
        'when_gets_good': llm_result.get('when_gets_good'),
        'ending_sentiment': llm_result.get('ending_sentiment'),
        'best_content_type': llm_result.get('best_content', {}).get('type'),
        'best_content_description': llm_result.get('best_content', {}).get('description'),
        'viewer_appeal': llm_result.get('viewer_appeal'),
        'total_discussions': relevant_count,
        'last_calculated': datetime.now().isoformat()
    }
    
    supabase.table('show_statistics')\
        .update(update_data)\
        .eq('show_id', 50)\
        .execute()
    
    print("✓ Updated LOTR successfully!")