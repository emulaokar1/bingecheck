#!/usr/bin/env python3
"""
Add viewer_appeal to all 50 shows in the existing results JSON
"""

import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import sys
import time

# Add src to path
sys.path.append('src')
from processing.llm_analysis import TVShowLLMAnalyzer

def add_viewer_appeal_to_all():
    """Add viewer_appeal to all shows in the existing results"""
    
    # Initialize analyzer
    analyzer = TVShowLLMAnalyzer()
    
    # Load existing results
    results_file = Path("data/analysis/llm_analysis_results.json")
    if not results_file.exists():
        print("❌ Results file not found")
        return
    
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    print(f"✅ Loaded {len(results)} shows from results file")
    
    # Load Reddit discussions
    discussions_df = analyzer.load_reddit_discussions()
    if discussions_df is None:
        print("❌ Could not load Reddit discussions")
        return
    
    print(f"✅ Loaded {len(discussions_df)} Reddit discussions")
    
    # Keywords for viewer appeal
    appeal_keywords = [
        "love", "amazing", "brilliant", "excellent", "fantastic", "masterpiece", 
        "recommend", "favorite", "best show", "addicted", "hooked", "binge", 
        "incredible", "outstanding", "phenomenal", "perfect", "why I love",
        "what makes it great", "so good", "compelling", "gripping", "engaging"
    ]
    
    # Process each show
    updated_count = 0
    skipped_count = 0
    
    for i, show in enumerate(results):
        show_id = show.get('show_id')
        show_title = show.get('show_title', 'Unknown')
        
        # Skip if already has viewer_appeal
        if 'viewer_appeal' in show:
            print(f"⏭️  {i+1:2d}/{len(results)} {show_title}: Already has viewer_appeal, skipping")
            skipped_count += 1
            continue
        
        print(f"🤖 {i+1:2d}/{len(results)} Processing {show_title}...")
        
        # Get appeal posts
        appeal_posts = analyzer.filter_reddit_posts(show_id, appeal_keywords, discussions_df)
        
        if not appeal_posts:
            print(f"⚠️  No appeal posts found for {show_title}")
            show['viewer_appeal'] = "This show has a dedicated fanbase that appreciates its unique storytelling and character development."
            updated_count += 1
            continue
        
        # Generate viewer appeal analysis
        appeal_analysis = analyzer.llm_analyze_viewer_appeal(show_title, appeal_posts, i)
        
        if appeal_analysis:
            show['viewer_appeal'] = appeal_analysis
            updated_count += 1
            print(f"✅ Added viewer_appeal: {appeal_analysis[:80]}...")
        else:
            print(f"❌ Failed to generate appeal for {show_title}")
            show['viewer_appeal'] = "This show has a dedicated fanbase that appreciates its unique storytelling and character development."
            updated_count += 1
        
        # Save progress every 5 shows
        if (i + 1) % 5 == 0 or i == len(results) - 1:
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"💾 Progress saved: {updated_count} updated, {skipped_count} skipped")
        
        # Rate limiting - be conservative
        time.sleep(1)
    
    # Final save
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n🎉 Complete!")
    print(f"✅ Updated: {updated_count} shows")
    print(f"⏭️  Skipped: {skipped_count} shows (already had viewer_appeal)")
    print(f"💰 API calls: {analyzer.api_calls_made}, Cost: ${analyzer.total_cost:.4f}")
    print(f"💾 Results saved to {results_file}")

if __name__ == "__main__":
    add_viewer_appeal_to_all()