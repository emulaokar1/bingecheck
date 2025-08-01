#!/usr/bin/env python3
"""
Improved Reddit Show Mapping Fix
Uses subreddit context as primary signal, text analysis as fallback
"""

import pandas as pd
import json
import re
from pathlib import Path
from collections import Counter

class ImprovedShowMappingFixer:
    def __init__(self):
        self.discussions_df = None
        self.show_mappings = {}  # show_id -> show_title
        self.subreddit_mappings = {}  # subreddit -> show_id
        self.corrections_made = 0
        
    def load_data(self):
        """Load discussions and show mappings"""
        discussions_path = Path("data/processed/reddit_discussions.csv")
        peak_analysis_path = Path("data/analysis/peak_analysis_results.json")
        
        if not discussions_path.exists():
            print(f"❌ Discussions file not found: {discussions_path}")
            return False
            
        self.discussions_df = pd.read_csv(discussions_path)
        
        # Load show mappings from peak analysis
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
    
    def create_subreddit_mappings(self):
        """Create mappings between subreddits and shows"""
        # Manual mappings for clear cases
        manual_mappings = {
            'gameofthrones': 'Game of Thrones',
            'breakingbad': 'Breaking Bad',
            'bettercallsaul': 'Better Call Saul',
            'strangerthings': 'Stranger Things',
            'westworld': 'Westworld',
            'theoffice': 'The Office',
            'community': 'Community',
            'friends': 'Friends',
            'sherlock': 'Sherlock',
            'house': 'House',
            'dexter': 'Dexter',
            'supernatural': 'Supernatural',
            'lost': 'Lost',
            'thewire': 'The Wire',
            'narcos': 'Narcos',
            'chernobyl': 'Chernobyl',
            'fargo': 'Fargo',
            'dark': 'Dark',
            'wednesday': 'Wednesday',
            'daredevil': 'Daredevil',
            'loki': 'Loki',
            'arrow': 'Arrow',
            'suits': 'Suits',
            'blackmirror': 'Black Mirror',
            'theboys': 'The Boys',
            'ozark': 'Ozark',
            'mindhunter': 'Mindhunter',
            'walkingdead': 'The Walking Dead',
        }
        
        # Convert manual mappings to show_id mappings
        for subreddit, show_title in manual_mappings.items():
            # Find the show_id for this title
            for show_id, title in self.show_mappings.items():
                if title.lower() == show_title.lower():
                    self.subreddit_mappings[subreddit.lower()] = show_id
                    break
        
        print(f"✅ Created {len(self.subreddit_mappings)} subreddit mappings")
        
        # Show what we mapped
        print("📍 Subreddit mappings:")
        for subreddit, show_id in list(self.subreddit_mappings.items())[:10]:
            show_title = self.show_mappings.get(show_id, 'Unknown')
            print(f"   r/{subreddit} -> {show_title}")
        if len(self.subreddit_mappings) > 10:
            print(f"   ... and {len(self.subreddit_mappings) - 10} more")
    
    def analyze_post_with_subreddit(self, post):
        """Analyze post using subreddit context first, then content"""
        subreddit = str(post.get('subreddit', '')).lower()
        
        # 1. Check if subreddit directly maps to a show
        if subreddit in self.subreddit_mappings:
            return self.subreddit_mappings[subreddit]
        
        # 2. For generic subreddits, use text analysis
        generic_subreddits = {
            'television', 'askreddit', 'netflix', 'hbo', 'tv', 'televisiondiscussion',
            'streaming', 'movies', 'entertainment', 'popculture'
        }
        
        if subreddit in generic_subreddits or not subreddit:
            return self.analyze_post_content(post)
        
        # 3. For unknown subreddits, use text analysis as fallback
        return self.analyze_post_content(post)
    
    def generate_abbreviations(self, show_title):
        """Generate common abbreviations for a show title"""
        title_lower = show_title.lower()
        abbreviations = []
        
        # Manual mappings for known cases (only safe, unambiguous abbreviations)
        manual_abbrevs = {
            "game of thrones": [r'\bgot\b', r'\bg\.o\.t\b'],
            "breaking bad": [r'\bbreaking bad\b'],  # Remove 'bb' - too ambiguous
            "stranger things": [r'\bstranger things\b'],  # Remove 'st' - too ambiguous  
            "better call saul": [r'\bbcs\b', r'\bbetter call saul\b'],
            "the walking dead": [r'\btwd\b', r'\bwalking dead\b'],
            "how i met your mother": [r'\bhimym\b', r'\bhow i met your mother\b'],
            "it's always sunny in philadelphia": [r'\biasip\b', r'\balways sunny\b'],
            "american horror story": [r'\bahs\b', r'\bamerican horror story\b'],
            "house of cards": [r'\bhoc\b', r'\bhouse of cards\b'],
            "parks and recreation": [r'\bparks and rec\b', r'\bp&r\b', r'\bparks and recreation\b'],
            "orange is the new black": [r'\boitnb\b', r'\borange is the new black\b'],
            "attack on titan": [r'\baot\b', r'\battack on titan\b'],
            "the big bang theory": [r'\btbbt\b', r'\bbig bang theory\b'],
            "law & order": [r'\bl&o\b', r'\blaw and order\b'],
        }
        
        if title_lower in manual_abbrevs:
            abbreviations.extend(manual_abbrevs[title_lower])
        
        # Auto-generate initials for multi-word titles (only if 3+ letters to avoid ambiguity)
        words = title_lower.split()
        if len(words) >= 3:  # Only for 3+ word titles
            # Skip common articles/prepositions
            skip_words = {'the', 'a', 'an', 'of', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'with', 'by'}
            meaningful_words = [w for w in words if w not in skip_words and len(w) > 0]
            
            if len(meaningful_words) >= 3:  # Need at least 3 meaningful words
                # Generate initials (e.g., "How I Met Your Mother" -> "himym")
                initials = ''.join([w[0] for w in meaningful_words])
                if len(initials) >= 4:  # Only if 4+ character abbreviation to avoid ambiguity
                    abbreviations.append(rf'\b{re.escape(initials)}\b')
        
        return abbreviations
    
    def analyze_post_content(self, post):
        """Analyze post content for show mentions (fallback method)"""
        title = str(post.get('title', ''))
        content = str(post.get('content', ''))
        combined_text = f"{title} {content}".lower()
        
        # Count mentions of each show
        mentions = {}
        for show_id, show_title in self.show_mappings.items():
            title_lower = show_title.lower()
            count = 0
            
            # Check exact title match
            if title_lower in combined_text:
                count += combined_text.count(title_lower)
            
            # Check abbreviations
            abbreviations = self.generate_abbreviations(show_title)
            for abbrev_pattern in abbreviations:
                matches = re.findall(abbrev_pattern, combined_text)
                count += len(matches)
            
            if count > 0:
                mentions[show_id] = count
        
        if not mentions:
            return None
        
        # Return the most mentioned show
        return max(mentions.items(), key=lambda x: x[1])[0]
    
    def find_posts_to_correct(self):
        """Find posts that should be corrected using the new algorithm"""
        corrections = []
        
        for idx, post in self.discussions_df.iterrows():
            current_show_id = post['show_id']
            predicted_show_id = self.analyze_post_with_subreddit(post)
            
            if predicted_show_id and predicted_show_id != current_show_id:
                corrections.append({
                    'index': idx,
                    'reddit_id': post['reddit_id'],
                    'title': post['title'][:80] + "..." if len(str(post['title'])) > 80 else post['title'],
                    'subreddit': post['subreddit'],
                    'current_show_id': current_show_id,
                    'current_show': self.show_mappings.get(current_show_id, 'Unknown'),
                    'predicted_show_id': predicted_show_id,
                    'predicted_show': self.show_mappings.get(predicted_show_id, 'Unknown')
                })
        
        return corrections
    
    def apply_corrections(self, corrections):
        """Apply the corrections"""
        if not corrections:
            print("No corrections needed!")
            return
        
        print(f"\n🔍 Found {len(corrections)} posts to correct")
        
        # Show sample corrections
        print("\nSample corrections:")
        for correction in corrections[:10]:
            print(f"  '{correction['title']}'")
            print(f"    r/{correction['subreddit']} | {correction['current_show']} → {correction['predicted_show']}")
        
        if len(corrections) > 10:
            print(f"  ... and {len(corrections) - 10} more")
        
        # Apply corrections
        for correction in corrections:
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
        
        original_path = Path("data/processed/reddit_discussions.csv")
        self.discussions_df.to_csv(original_path, index=False)
        print(f"💾 Saved corrected data to {original_path}")
    
    def run_fix(self):
        """Run the improved fix pipeline"""
        print("🚀 Starting improved Reddit show mapping fix...")
        
        if not self.load_data():
            return
        
        self.create_subreddit_mappings()
        corrections = self.find_posts_to_correct()
        self.apply_corrections(corrections)
        self.save_corrected_data()
        
        print(f"\n📊 Fix complete - corrected {self.corrections_made} posts")

def main():
    fixer = ImprovedShowMappingFixer()
    fixer.run_fix()

if __name__ == "__main__":
    main()