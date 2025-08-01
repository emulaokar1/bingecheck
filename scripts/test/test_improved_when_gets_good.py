#!/usr/bin/env python3
"""
Test script for improved "when it gets good" analysis
Tests on Game of Thrones and Breaking Bad to see if we get earlier recommendations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.processing.llm_analysis import TVShowLLMAnalyzer
import json
from pathlib import Path

def test_improved_analysis():
    """Test the improved when it gets good analysis"""
    print("🧪 Testing Improved 'When It Gets Good' Analysis")
    print("=" * 60)
    
    analyzer = TVShowLLMAnalyzer()
    
    # Load data
    peak_analysis = analyzer.load_peak_analysis()
    discussions_df = analyzer.load_reddit_discussions()
    
    if not peak_analysis or discussions_df is None:
        print("❌ Could not load required data")
        return
    
    # Test specific shows
    test_shows = ["Game of Thrones", "Breaking Bad", "Stranger Things"]
    
    for show_title in test_shows:
        print(f"\n📺 Testing: {show_title}")
        print("-" * 40)
        
        # Find show in peak analysis
        show_analysis = None
        for show in peak_analysis:
            if show.get('show_title') == show_title:
                show_analysis = show
                break
        
        if not show_analysis:
            print(f"❌ Show not found: {show_title}")
            continue
        
        show_id = show_analysis.get('show_id')
        print(f"Show ID: {show_id}")
        
        # Check season analysis data
        season_analysis = show_analysis.get('season_analysis', [])
        print(f"Season data available: {len(season_analysis)} seasons")
        
        # Show season ratings for context
        if season_analysis:
            print("Season ratings:")
            for season in season_analysis[:4]:  # First 4 seasons
                season_num = season.get('season', 'N/A')
                avg_rating = season.get('avg_rating', 0)
                episode_count = season.get('episode_count', 0)
                print(f"  Season {season_num}: {avg_rating:.2f} avg ({episode_count} episodes)")
        
        # Test improvement point detection
        improvement_point = analyzer.find_early_improvement_point(season_analysis)
        if improvement_point:
            season = improvement_point.get('season', 1)  
            episode = improvement_point.get('episode', 1)
            rating = improvement_point.get('rating', 0)
            improvement = improvement_point.get('improvement_from_s1', 0)
            print(f"📈 Improvement point: Season {season}, Episode {episode}")
            print(f"   Rating: {rating:.2f} (+{improvement:.2f} from S1)")
        else:
            print("📈 No clear improvement point found")
        
        # Test Reddit keyword filtering
        when_good_keywords = [
            "gets good", "worth watching", "should I continue", "stick with it", 
            "slow start", "boring at first", "gets better after", "improves", 
            "first season slow", "when does it pick up", "worth pushing through",
            "skip first season", "starts slow", "hard to get into", "give it time",
            "patient with", "rough start", "first episodes boring", "when to stop watching",
            "does it get better", "struggle through", "payoff", "investment"
        ]
        
        when_good_posts = analyzer.filter_reddit_posts(show_id, when_good_keywords, discussions_df)
        print(f"🗣️  Found {len(when_good_posts)} relevant Reddit posts")
        
        if when_good_posts:
            print("Sample post titles:")
            for post in when_good_posts[:3]:
                title = post.get('title', 'No title')[:80] + "..."
                score = post.get('score', 0)
                print(f"  • ({score} pts) {title}")
        
        print()
    
    print("✅ Improvement point detection test completed!")

if __name__ == "__main__":
    test_improved_analysis()