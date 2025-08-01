#!/usr/bin/env python3
"""
Test script to add viewer_appeal to Game of Thrones only
"""

import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import sys

# Add src to path
sys.path.append('src')
from processing.llm_analysis import TVShowLLMAnalyzer

def test_viewer_appeal_got():
    """Test viewer appeal analysis on Game of Thrones only"""
    
    # Initialize analyzer
    analyzer = TVShowLLMAnalyzer()
    
    # Load peak analysis for Game of Thrones (show_id = 6)
    peak_analysis = analyzer.load_peak_analysis()
    if not peak_analysis:
        print("❌ Could not load peak analysis")
        return
    
    # Find Game of Thrones
    got_analysis = None
    for show in peak_analysis:
        if show.get('show_id') == 6 and show.get('show_title') == 'Game of Thrones':
            got_analysis = show
            break
    
    if not got_analysis:
        print("❌ Could not find Game of Thrones in peak analysis")
        return
    
    print(f"✅ Found Game of Thrones: {got_analysis['show_title']}")
    
    # Load Reddit discussions
    discussions_df = analyzer.load_reddit_discussions()
    if discussions_df is None:
        print("❌ Could not load Reddit discussions")
        return
    
    print(f"✅ Loaded {len(discussions_df)} Reddit discussions")
    
    # Test the viewer appeal analysis specifically
    appeal_keywords = [
        "love", "amazing", "brilliant", "excellent", "fantastic", "masterpiece", 
        "recommend", "favorite", "best show", "addicted", "hooked", "binge", 
        "incredible", "outstanding", "phenomenal", "perfect", "why I love",
        "what makes it great", "so good", "compelling", "gripping", "engaging"
    ]
    
    appeal_posts = analyzer.filter_reddit_posts(6, appeal_keywords, discussions_df)
    print(f"✅ Found {len(appeal_posts)} relevant posts for viewer appeal")
    
    if appeal_posts:
        print("\n📝 Sample posts for viewer appeal:")
        for i, post in enumerate(appeal_posts[:3]):
            print(f"{i+1}. {post['title'][:80]}...")
    
    # Run the analysis
    print(f"\n🤖 Running viewer appeal analysis for Game of Thrones...")
    appeal_analysis = analyzer.llm_analyze_viewer_appeal("Game of Thrones", appeal_posts, 0)
    
    if appeal_analysis:
        print(f"✅ Generated viewer appeal: {appeal_analysis}")
        
        # Load existing results
        results_file = Path("data/analysis/llm_analysis_results.json")
        if results_file.exists():
            with open(results_file, 'r') as f:
                results = json.load(f)
            
            # Find and update Game of Thrones entry
            for show in results:
                if show.get('show_id') == 6 and show.get('show_title') == 'Game of Thrones':
                    show['viewer_appeal'] = appeal_analysis
                    print(f"✅ Added viewer_appeal to Game of Thrones in results")
                    break
            
            # Save updated results
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"✅ Saved updated results to {results_file}")
            print(f"\n💰 Used {analyzer.api_calls_made} API calls, cost: ${analyzer.total_cost:.4f}")
            
        else:
            print("❌ Results file not found")
    else:
        print("❌ Failed to generate viewer appeal analysis")

if __name__ == "__main__":
    test_viewer_appeal_got()