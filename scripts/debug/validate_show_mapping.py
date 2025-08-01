#!/usr/bin/env python3
"""
Validate Reddit Show Mapping Quality
Checks if posts are correctly assigned to shows after the fix
"""

import pandas as pd
import json
import re
from pathlib import Path
from collections import Counter, defaultdict
import random

class ShowMappingValidator:
    def __init__(self):
        self.discussions_df = None
        self.show_mappings = {}
        
    def load_data(self):
        """Load discussions and show mappings"""
        discussions_path = Path("data/processed/reddit_discussions.csv")
        peak_analysis_path = Path("data/analysis/peak_analysis_results.json")
        
        if not discussions_path.exists():
            print(f"❌ Discussions file not found: {discussions_path}")
            return False
            
        self.discussions_df = pd.read_csv(discussions_path)
        
        # Load show mappings
        if peak_analysis_path.exists():
            with open(peak_analysis_path, 'r') as f:
                peak_data = json.load(f)
            
            for show in peak_data:
                show_id = show.get('show_id')
                show_title = show.get('show_title')
                if show_id and show_title:
                    self.show_mappings[show_id] = show_title
        
        print(f"✅ Loaded {len(self.discussions_df)} posts and {len(self.show_mappings)} shows")
        return True
    
    def check_specific_examples(self):
        """Check specific known examples"""
        print("\n🔍 Checking specific examples:")
        
        examples = [
            {"reddit_id": "k9t5ub", "expected_show": "Game of Thrones", "expected_id": 6},
        ]
        
        for example in examples:
            posts = self.discussions_df[self.discussions_df['reddit_id'] == example['reddit_id']]
            
            if len(posts) == 0:
                print(f"❌ Post {example['reddit_id']} not found")
                continue
                
            post = posts.iloc[0]
            actual_show_id = post['show_id']
            actual_show = self.show_mappings.get(actual_show_id, 'Unknown')
            
            if actual_show_id == example['expected_id']:
                print(f"✅ {example['reddit_id']}: '{post['title'][:50]}...' -> {actual_show} (ID: {actual_show_id})")
            else:
                print(f"❌ {example['reddit_id']}: Expected {example['expected_show']} (ID: {example['expected_id']}), got {actual_show} (ID: {actual_show_id})")
    
    def sample_validation(self, n_samples=20):
        """Manually validate a random sample of posts"""
        print(f"\n🎲 Random sample validation ({n_samples} posts):")
        
        # Sample posts from different shows
        sample_posts = []
        
        # Get a few posts from each show
        for show_id in list(self.show_mappings.keys())[:10]:  # First 10 shows
            show_posts = self.discussions_df[self.discussions_df['show_id'] == show_id]
            if len(show_posts) > 0:
                # Sample 2 posts per show
                n_sample = min(2, len(show_posts))
                sampled = show_posts.sample(n=n_sample, random_state=42)
                sample_posts.extend(sampled.to_dict('records'))
        
        # Validate each sample
        correct = 0
        total = 0
        
        for post in sample_posts[:n_samples]:
            title = str(post.get('title', ''))
            content = str(post.get('content', ''))[:200]  # First 200 chars
            assigned_show_id = post['show_id']
            assigned_show = self.show_mappings.get(assigned_show_id, 'Unknown')
            
            # Check if the assigned show is mentioned in the content
            combined_text = f"{title} {content}".lower()
            assigned_show_lower = assigned_show.lower()
            
            # Basic validation: does the post mention the assigned show?
            show_mentioned = False
            
            # Check exact title match
            if assigned_show_lower in combined_text:
                show_mentioned = True
            
            # Check common abbreviations
            if assigned_show_lower == "game of thrones" and ("got " in combined_text or "g.o.t" in combined_text):
                show_mentioned = True
            elif assigned_show_lower == "breaking bad" and " bb " in combined_text:
                show_mentioned = True
            elif assigned_show_lower == "stranger things" and " st " in combined_text:
                show_mentioned = True
                
            if show_mentioned:
                correct += 1
                status = "✅"
            else:
                status = "❓"
            
            total += 1
            
            print(f"{status} {assigned_show} | '{title[:60]}...'" + 
                  (f" | Content: '{content[:50]}...'" if content.strip() else ""))
        
        accuracy = (correct / total) * 100 if total > 0 else 0
        print(f"\n📊 Sample accuracy: {correct}/{total} ({accuracy:.1f}%)")
        return accuracy
    
    def analyze_show_distribution(self):
        """Analyze the distribution of posts per show"""
        print("\n📈 Post distribution by show:")
        
        show_counts = self.discussions_df['show_id'].value_counts().head(10)
        
        for show_id, count in show_counts.items():
            show_name = self.show_mappings.get(show_id, f'Unknown (ID: {show_id})')
            percentage = (count / len(self.discussions_df)) * 100
            print(f"  {show_name}: {count} posts ({percentage:.1f}%)")
    
    def find_potential_issues(self):
        """Find posts that might still be incorrectly mapped"""
        print("\n🚨 Potential mapping issues:")
        
        issues = []
        
        # Check for posts that mention multiple shows
        for idx, post in self.discussions_df.iterrows():
            title = str(post.get('title', ''))
            content = str(post.get('content', ''))[:500]  # First 500 chars
            combined = f"{title} {content}".lower()
            
            assigned_show_id = post['show_id']
            assigned_show = self.show_mappings.get(assigned_show_id, 'Unknown')
            
            # Count mentions of other shows
            other_shows_mentioned = []
            for other_id, other_show in self.show_mappings.items():
                if other_id != assigned_show_id and other_show.lower() in combined:
                    other_shows_mentioned.append(other_show)
            
            # If this post mentions other shows more prominently, it might be misassigned
            if len(other_shows_mentioned) > 0 and assigned_show.lower() not in combined:
                issues.append({
                    'reddit_id': post['reddit_id'],
                    'title': title[:80] + "..." if len(title) > 80 else title,
                    'assigned_show': assigned_show,
                    'other_shows': other_shows_mentioned
                })
        
        # Show first 10 potential issues
        for issue in issues[:10]:
            print(f"❓ '{issue['title']}'")
            print(f"   Assigned: {issue['assigned_show']}")
            print(f"   Also mentions: {', '.join(issue['other_shows'])}")
            print()
        
        if len(issues) > 10:
            print(f"... and {len(issues) - 10} more potential issues")
        
        issue_rate = (len(issues) / len(self.discussions_df)) * 100
        print(f"📊 Potential issue rate: {len(issues)}/{len(self.discussions_df)} ({issue_rate:.1f}%)")
        
        return issues
    
    def run_validation(self):
        """Run complete validation suite"""
        print("🔍 Starting show mapping validation...")
        
        if not self.load_data():
            return
        
        self.check_specific_examples()
        accuracy = self.sample_validation()
        self.analyze_show_distribution()
        issues = self.find_potential_issues()
        
        print(f"\n📋 Validation Summary:")
        print(f"   Sample accuracy: {accuracy:.1f}%")
        print(f"   Potential issues: {len(issues)} posts ({(len(issues)/len(self.discussions_df)*100):.1f}%)")
        
        if accuracy > 85 and len(issues) < len(self.discussions_df) * 0.05:
            print("✅ Mapping quality looks good!")
        elif accuracy > 70:
            print("⚠️  Mapping quality is decent but could be improved")
        else:
            print("❌ Mapping quality needs improvement")

def main():
    validator = ShowMappingValidator()
    validator.run_validation()

if __name__ == "__main__":
    main()