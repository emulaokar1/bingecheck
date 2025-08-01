#!/usr/bin/env python3
"""
Debug script to find the exact issue in LLM analysis
"""

import json
import pandas as pd
from pathlib import Path

def debug_show_data():
    """Debug the show data structure"""
    
    # Load data
    with open('data/analysis/peak_analysis_results.json', 'r') as f:
        peak_analysis = json.load(f)
    
    discussions_df = pd.read_csv('data/processed/reddit_discussions.csv')
    
    # Check first 3 shows
    for i, show in enumerate(peak_analysis[:3]):
        print(f"\n=== Show {i+1}: {show['show_title']} ===")
        print(f"Show ID: {show.get('show_id')}")
        print(f"Peak episodes: {len(show.get('peak_episodes', []))} found")
        print(f"Quality runs: {len(show.get('quality_runs', []))} found")
        
        # Check Reddit discussions for this show
        show_discussions = discussions_df[discussions_df['show_id'] == show['show_id']]
        print(f"Reddit discussions: {len(show_discussions)} found")
        
        if len(show_discussions) > 0:
            print(f"Sample discussion columns: {list(show_discussions.columns)}")
            sample_post = show_discussions.iloc[0]
            print(f"Sample post title type: {type(sample_post.get('title'))}")
            print(f"Sample post content type: {type(sample_post.get('content'))}")
            print(f"Sample post score type: {type(sample_post.get('score'))}")
        
        # Check peak episodes structure
        if show.get('peak_episodes'):
            peak = show['peak_episodes'][0]
            print(f"First peak structure: {peak}")
            print(f"Peak keys: {list(peak.keys())}")
        
        # Check quality runs structure
        if show.get('quality_runs'):
            run = show['quality_runs'][0]
            print(f"First run structure: {run}")
            print(f"Run keys: {list(run.keys())}")

if __name__ == "__main__":
    debug_show_data()