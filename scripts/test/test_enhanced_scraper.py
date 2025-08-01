#!/usr/bin/env python3
"""
Test script for the enhanced Reddit scraper
Tests on a small subset of shows to verify functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_collection.reddit_scraper import OvernightRedditScraper
import json
from pathlib import Path

def test_enhanced_scraper():
    """Test the enhanced scraper on a few shows"""
    print("🧪 Testing Enhanced Reddit Scraper")
    print("=" * 50)
    
    scraper = OvernightRedditScraper()
    
    # Test shows (manually selected)
    test_shows = [
        {'id': 6, 'title': 'Game of Thrones'},
        {'id': 7, 'title': 'Breaking Bad'},
        {'id': 8, 'title': 'Stranger Things'}
    ]
    
    all_discussions = []
    
    for i, show in enumerate(test_shows, 1):
        print(f"\n📺 Testing show {i}/{len(test_shows)}: {show['title']}")
        print("-" * 40)
        
        try:
            discussions = scraper.search_show_discussions(show['title'], show['id'], limit_per_search=10)
            
            if discussions:
                all_discussions.extend(discussions)
                print(f"✅ Found {len(discussions)} discussions")
                
                # Show confidence breakdown for this show
                confidence_counts = {'HIGH_CONFIDENCE': 0, 'MEDIUM_CONFIDENCE': 0, 'LOW_CONFIDENCE': 0, 'UNCERTAIN': 0}
                for disc in discussions:
                    tier = disc.get('tier', 'UNCERTAIN')
                    confidence_counts[tier] += 1
                
                print(f"📊 Confidence breakdown:")
                print(f"   HIGH:   {confidence_counts['HIGH_CONFIDENCE']}")
                print(f"   MEDIUM: {confidence_counts['MEDIUM_CONFIDENCE']}")
                print(f"   LOW:    {confidence_counts['LOW_CONFIDENCE']}")
                print(f"   UNCERTAIN: {confidence_counts['UNCERTAIN']}")
                
                # Show sample posts
                print(f"\n🔍 Sample posts:")
                for disc in discussions[:3]:
                    conf = disc.get('confidence', 0)
                    tier = disc.get('tier', 'UNCERTAIN')
                    reason = disc.get('assignment_reason', 'unknown')
                    subreddit = disc.get('subreddit', 'unknown')
                    title = disc.get('title', 'No title')[:60] + "..."
                    
                    print(f"   • r/{subreddit} | {tier} ({conf:.2f}) | {reason}")
                    print(f"     '{title}'")
            else:
                print(f"ℹ️  No discussions found for {show['title']}")
                
        except Exception as e:
            print(f"❌ Error testing {show['title']}: {e}")
            continue
    
    # Overall summary
    print(f"\n🎉 Test Complete!")
    print(f"📊 Total discussions found: {len(all_discussions)}")
    
    if all_discussions:
        # Overall confidence distribution
        overall_confidence = {'HIGH_CONFIDENCE': 0, 'MEDIUM_CONFIDENCE': 0, 'LOW_CONFIDENCE': 0, 'UNCERTAIN': 0}
        assignment_reasons = {}
        
        for disc in all_discussions:
            tier = disc.get('tier', 'UNCERTAIN')
            overall_confidence[tier] += 1
            
            reason = disc.get('assignment_reason', 'unknown')
            assignment_reasons[reason] = assignment_reasons.get(reason, 0) + 1
        
        print(f"\n📈 Overall Confidence Distribution:")
        for tier, count in overall_confidence.items():
            percentage = (count / len(all_discussions)) * 100
            print(f"   {tier}: {count} ({percentage:.1f}%)")
        
        print(f"\n🔍 Assignment Reasons:")
        for reason, count in assignment_reasons.items():
            percentage = (count / len(all_discussions)) * 100
            print(f"   {reason}: {count} ({percentage:.1f}%)")
        
        # Save test results
        test_results = {
            'total_discussions': len(all_discussions),
            'confidence_distribution': overall_confidence,
            'assignment_reasons': assignment_reasons,
            'sample_posts': all_discussions[:10]  # Save first 10 as examples
        }
        
        results_path = Path("data/test_enhanced_scraper_results.json")
        with open(results_path, 'w') as f:
            json.dump(test_results, f, indent=2, default=str)
        
        print(f"💾 Test results saved to: {results_path}")
    
    print(f"\n✅ Enhanced scraper test completed successfully!")

if __name__ == "__main__":
    test_enhanced_scraper()