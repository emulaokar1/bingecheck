#!/usr/bin/env python3
"""
Fix Game of Thrones missing 'when_gets_good' data
"""

import sys
import os
import json
import pandas as pd
from pathlib import Path

# Add src to path to import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from processing.llm_analysis import TVShowLLMAnalyzer

def fix_game_of_thrones_data():
    """Fix the missing when_gets_good data for Game of Thrones"""
    
    print("🔧 Fixing Game of Thrones 'when_gets_good' data...")
    
    # Initialize analyzer
    analyzer = TVShowLLMAnalyzer()
    
    # Load existing LLM analysis results
    llm_results_file = Path("data/analysis/llm_analysis_results.json")
    if not llm_results_file.exists():
        print("❌ LLM analysis results file not found")
        return
    
    with open(llm_results_file, 'r') as f:
        llm_results = json.load(f)
    
    # Load peak analysis to get season data for GoT
    peak_analysis_file = Path("data/analysis/peak_analysis_results.json")
    if not peak_analysis_file.exists():
        print("❌ Peak analysis results file not found")
        return
    
    with open(peak_analysis_file, 'r') as f:
        peak_analysis = json.load(f)
    
    # Load Reddit discussions
    discussions_df = analyzer.load_reddit_discussions()
    if discussions_df is None:
        print("❌ Could not load Reddit discussions")
        return
    
    # Find Game of Thrones in peak analysis
    got_peak_analysis = None
    for show in peak_analysis:
        if show.get('show_id') == 6 and 'Game of Thrones' in show.get('show_title', ''):
            got_peak_analysis = show
            break
    
    if not got_peak_analysis:
        print("❌ Could not find Game of Thrones in peak analysis")
        return
    
    print(f"✅ Found Game of Thrones peak analysis: {got_peak_analysis.get('show_title')}")
    
    # Find Game of Thrones in LLM results
    got_llm_result = None
    got_index = -1
    for i, show in enumerate(llm_results):
        if show.get('show_id') == 6:
            got_llm_result = show
            got_index = i
            break
    
    if not got_llm_result:
        print("❌ Could not find Game of Thrones in LLM results")
        return
    
    print(f"✅ Found Game of Thrones LLM result at index {got_index}")
    
    # Check if it already has when_gets_good
    if 'when_gets_good' in got_llm_result:
        print("⚠️  Game of Thrones already has 'when_gets_good' data:")
        print(f"   {got_llm_result['when_gets_good']}")
        return
    
    # Filter Reddit posts for "when_gets_good" keywords
    when_good_keywords = [
        "gets good", "worth watching", "should I continue", "stick with it", 
        "slow start", "boring at first", "gets better after", "improves", 
        "first season slow", "when does it pick up", "worth pushing through",
        "skip first season", "starts slow", "hard to get into", "give it time",
        "patient with", "rough start", "first episodes boring", "when to stop watching",
        "does it get better", "struggle through", "payoff", "investment"
    ]
    
    when_good_posts = analyzer.filter_reddit_posts(6, when_good_keywords, discussions_df)
    print(f"📊 Found {len(when_good_posts)} relevant Reddit posts for Game of Thrones")
    
    if len(when_good_posts) == 0:
        print("⚠️  No relevant Reddit posts found. Let's try broader keywords...")
        # Try broader keywords
        broader_keywords = ["good", "better", "worth", "continue", "watch"]
        when_good_posts = analyzer.filter_reddit_posts(6, broader_keywords, discussions_df, max_posts=5)
        print(f"📊 Found {len(when_good_posts)} posts with broader keywords")
    
    if len(when_good_posts) > 0:
        # Show what posts we found
        print("📋 Top Reddit posts found:")
        for i, post in enumerate(when_good_posts[:3]):
            print(f"   {i+1}. {post['title'][:80]}... (Score: {post['score']})")
        
        # Get season analysis for trajectory
        season_analysis = got_peak_analysis.get('season_analysis', [])
        
        # Run LLM analysis
        print("🤖 Running LLM analysis for 'when_gets_good'...")
        when_good_analysis = analyzer.llm_analyze_when_gets_good(
            "Game of Thrones", 
            season_analysis,
            when_good_posts,
            0  # show_index
        )
        
        if when_good_analysis:
            print(f"✅ Generated analysis: {when_good_analysis}")
            
            # Add to LLM results
            got_llm_result['when_gets_good'] = when_good_analysis
            
            # Save updated results
            with open(llm_results_file, 'w') as f:
                json.dump(llm_results, f, indent=2)
            
            print("💾 Updated LLM analysis results saved!")
        else:
            print("❌ Failed to generate LLM analysis")
    else:
        print("❌ No suitable Reddit posts found for analysis")

if __name__ == "__main__":
    fix_game_of_thrones_data()