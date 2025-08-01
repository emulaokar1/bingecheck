#!/usr/bin/env python3
"""
Fix Reddit Post show_id Assignments
Analyzes post content to correctly assign show_ids based on which show is actually discussed
"""

import pandas as pd
import json
import re
from pathlib import Path
from collections import defaultdict, Counter

class RedditShowMappingFixer:
    def __init__(self):
        self.shows_df = None
        self.discussions_df = None
        self.show_mappings = {}
        self.corrections_made = 0
        
    def load_data(self):
        """Load shows and discussions data"""
        shows_path = Path("data/processed/shows.csv")
        discussions_path = Path("data/processed/reddit_discussions.csv")
        peak_analysis_path = Path("data/analysis/peak_analysis_results.json")
        
        if not shows_path.exists() or not discussions_path.exists():
            print(f"❌ Required files not found:")
            print(f"   Shows: {shows_path}")
            print(f"   Discussions: {discussions_path}")
            return False
            
        self.discussions_df = pd.read_csv(discussions_path)
        
        # Load show mappings from peak analysis file (has show_id -> show_title mapping)
        if peak_analysis_path.exists():
            with open(peak_analysis_path, 'r') as f:
                peak_data = json.load(f)
            
            for show in peak_data:
                show_id = show.get('show_id')
                show_title = show.get('show_title')
                if show_id and show_title:
                    self.show_mappings[show_id] = show_title
        else:
            print(f"❌ Peak analysis file not found: {peak_analysis_path}")
            return False
        
        print(f"✅ Loaded {len(self.show_mappings)} shows and {len(self.discussions_df)} discussions")
        return True
    
    def extract_show_mentions(self, text):
        """Extract explicit show mentions from text"""
        if pd.isna(text):
            return []
        
        text = str(text).lower()
        mentions = []
        
        # Check for each show title
        for show_id, title in self.show_mappings.items():
            # Create regex patterns for exact matches
            title_lower = title.lower()
            patterns = [
                rf'\b{re.escape(title_lower)}\b',  # Exact title match
            ]
            
            # Add common abbreviations/alternatives
            if title_lower == "game of thrones":
                patterns.extend([r'\bgot\b', r'\bg\.o\.t\b'])
            elif title_lower == "breaking bad":
                patterns.extend([r'\bbb\b'])
            elif title_lower == "stranger things":
                patterns.extend([r'\bst\b'])
            
            for pattern in patterns:
                if re.search(pattern, text):
                    mentions.append((show_id, title))
                    break
                    
        return mentions
    
    def analyze_post_content(self, post):
        """Analyze a single post to determine correct show_id"""
        title = str(post.get('title', ''))
        content = str(post.get('content', ''))
        combined_text = f"{title} {content}"
        
        # Extract all show mentions
        mentions = self.extract_show_mentions(combined_text)
        
        if not mentions:
            return None
        
        # Count mentions by show
        mention_counts = Counter([show_id for show_id, _ in mentions])
        
        # If only one show mentioned, that's likely correct
        if len(mention_counts) == 1:
            return list(mention_counts.keys())[0]
        
        # If multiple shows mentioned, prioritize by:
        # 1. Title mentions over content mentions
        title_mentions = self.extract_show_mentions(title)
        if len(title_mentions) == 1:
            return title_mentions[0][0]
        
        # 2. Most frequently mentioned show
        return mention_counts.most_common(1)[0][0]
    
    def find_misassigned_posts(self):
        """Find posts that are likely misassigned"""
        misassigned = []
        
        for idx, post in self.discussions_df.iterrows():
            current_show_id = post['show_id']
            predicted_show_id = self.analyze_post_content(post)
            
            if predicted_show_id and predicted_show_id != current_show_id:
                misassigned.append({
                    'index': idx,
                    'reddit_id': post['reddit_id'],
                    'title': post['title'][:100] + "..." if len(str(post['title'])) > 100 else post['title'],
                    'current_show_id': current_show_id,
                    'current_show': self.show_mappings.get(current_show_id, 'Unknown'),
                    'predicted_show_id': predicted_show_id,
                    'predicted_show': self.show_mappings.get(predicted_show_id, 'Unknown')
                })
        
        return misassigned
    
    def apply_corrections(self, misassigned):
        """Apply corrections to the dataset"""
        if not misassigned:
            print("No corrections needed!")
            return
        
        print(f"\n🔍 Found {len(misassigned)} potentially misassigned posts")
        
        # Apply corrections
        for correction in misassigned:
            idx = correction['index']
            new_show_id = correction['predicted_show_id']
            self.discussions_df.loc[idx, 'show_id'] = new_show_id
            self.corrections_made += 1
        
        print(f"✅ Applied {self.corrections_made} corrections")
    
    def save_corrected_data(self):
        """Save the corrected dataset"""
        if self.corrections_made == 0:
            print("No corrections were made")
            return
        
        # Save corrected data
        original_path = Path("data/processed/reddit_discussions.csv")
        self.discussions_df.to_csv(original_path, index=False)
        print(f"💾 Saved corrected data to {original_path}")
    
    def run_fix(self):
        """Run the complete fix pipeline"""
        print("🚀 Starting Reddit show mapping fix...")
        
        if not self.load_data():
            return
        
        misassigned = self.find_misassigned_posts()
        self.apply_corrections(misassigned)
        self.save_corrected_data()
        
        print(f"\n📊 Fix complete - corrected {self.corrections_made} posts")

def main():
    fixer = RedditShowMappingFixer()
    fixer.run_fix()

if __name__ == "__main__":
    main()