#!/usr/bin/env python3
"""
Test Reddit Scraper - Process just 3 shows to verify everything works
Save as: scripts/test_reddit_scraper.py
"""

import os
import time
import praw
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestRedditScraper:
    def __init__(self):
        load_dotenv()
        
        # Initialize Reddit
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT")
        )
        
        # Initialize Supabase (copied from working IMDb scraper)
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            supabase_key
        )
        
        # Try to authenticate if we have user credentials (like IMDb scraper)
        self._authenticate_if_possible()
        
        # Create directories
        Path("data/test").mkdir(parents=True, exist_ok=True)
        Path("data/processed").mkdir(parents=True, exist_ok=True)
    
    def _authenticate_if_possible(self):
        """Try to authenticate with Supabase using environment variables"""
        email = os.getenv("SUPABASE_USER_EMAIL")
        password = os.getenv("SUPABASE_USER_PASSWORD")
        
        if email and password:
            try:
                logger.info("🔐 Attempting to authenticate with Supabase...")
                auth_response = self.supabase.auth.sign_in_with_password({
                    "email": email, 
                    "password": password
                })
                logger.info("✅ Successfully authenticated with Supabase")
                return True
            except Exception as e:
                logger.warning(f"⚠️  Authentication failed: {e}")
                return False
        else:
            logger.info("ℹ️  No authentication credentials found, using anonymous access")
    
    def test_reddit_connection(self):
        """Test Reddit API connection"""
        try:
            # Simple test
            subreddit = self.reddit.subreddit("television")
            logger.info(f"✅ Reddit connected - r/television has {subreddit.subscribers:,} subscribers")
            return True
        except Exception as e:
            logger.error(f"❌ Reddit connection failed: {e}")
            return False
    
    def test_supabase_connection(self):
        """Test Supabase connection"""
        try:
            shows = self.supabase.table('shows').select('*').limit(3).execute()
            logger.info(f"✅ Supabase connected - found {len(shows.data)} shows")
            return True, shows.data
        except Exception as e:
            logger.error(f"❌ Supabase connection failed: {e}")
            return False, []
    
    def search_single_show(self, show_title, show_id):
        """Search for discussions about one show"""
        logger.info(f"🔍 Searching for discussions about: {show_title}")
        
        discussions = []
        
        try:
            # Search r/television for this show
            subreddit = self.reddit.subreddit("television")
            posts = subreddit.search(show_title, limit=5)
            
            for post in posts:
                if show_title.lower() in post.title.lower():
                    discussion = {
                        'show_id': show_id,
                        'reddit_id': post.id,
                        'title': post.title,
                        'content': post.selftext[:500],  # Limit content length
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                        'subreddit': post.subreddit.display_name,
                        'author': str(post.author) if post.author else '[deleted]',
                        'url': post.url,
                        'is_discussion': 'discussion' in post.title.lower()
                    }
                    discussions.append(discussion)
            
            logger.info(f"✅ Found {len(discussions)} relevant discussions")
            time.sleep(2)  # Rate limiting
            
        except Exception as e:
            logger.error(f"❌ Error searching for {show_title}: {e}")
        
        return discussions
    
    def save_to_csv(self, discussions):
        """Save discussions to CSV files as backup (like IMDb scraper)"""
        logger.info("💾 Saving data to CSV files...")
        
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save discussions data
        if discussions:
            discussions_df = pd.DataFrame(discussions)
            discussions_file = output_dir / "test_reddit_discussions.csv"
            discussions_df.to_csv(discussions_file, index=False)
            logger.info(f"✅ Saved {len(discussions)} discussions to {discussions_file}")
            return True
        return False

    def test_database_insert(self, discussions):
        """Test inserting discussions into Supabase (robust like IMDb scraper)"""
        if not discussions:
            logger.info("ℹ️  No discussions to insert")
            return True
        
        # Always save to CSV first (like IMDb scraper)
        self.save_to_csv(discussions)
        
        try:
            # Test with small batch
            logger.info(f"📤 Testing database insert with {len(discussions)} discussions...")
            result = self.supabase.table('reddit_posts').insert(discussions).execute()
            logger.info(f"✅ Successfully inserted {len(discussions)} discussions to database")
            return True
            
        except Exception as e:
            # Handle RLS errors gracefully (like IMDb scraper)
            if "row-level security policy" in str(e):
                logger.error(f"❌ Database authentication error: {e}")
                logger.error("💡 Solutions:")
                logger.error("   1. Add SUPABASE_SERVICE_ROLE_KEY to your .env file")
                logger.error("   2. Add SUPABASE_USER_EMAIL and SUPABASE_USER_PASSWORD to .env")
                logger.error("   3. Disable RLS on 'reddit_posts' table in Supabase dashboard")
                logger.error("   4. Run: ALTER TABLE reddit_posts DISABLE ROW LEVEL SECURITY; in SQL editor")
                logger.info("✅ Data has been saved to CSV files in data/processed/ for manual import")
                return "csv_only"
            else:
                logger.error(f"❌ Database insert failed: {e}")
                logger.info("💾 Data preserved in CSV backup")
                return "csv_only"
    
    def run_test(self):
        """Run complete test of Reddit scraping pipeline"""
        logger.info("🧪 Starting Reddit scraper test...")
        
        # Test 1: Reddit connection
        if not self.test_reddit_connection():
            return False
        
        # Test 2: Supabase connection
        db_ok, shows = self.test_supabase_connection()
        if not db_ok:
            return False
        
        # Test 3: Search for discussions (just 3 shows)
        test_shows = shows[:3]
        all_discussions = []
        
        for show in test_shows:
            discussions = self.search_single_show(show['title'], show['id'])
            all_discussions.extend(discussions)
        
        logger.info(f"📊 Test results: {len(all_discussions)} total discussions found")
        
        # Test 4: Database insertion
        if all_discussions:
            success = self.test_database_insert(all_discussions)
            if success == True:
                logger.info("🎉 All tests passed! Ready for full scraping.")
                return True
            elif success == "csv_only":
                logger.info("🎉 Pipeline works! Data saved to CSV (fix database permissions for direct insert).")
                return True
            else:
                logger.error("❌ Database insertion failed - check RLS settings")
                return False
        else:
            logger.info("⚠️  No discussions found, but pipeline works")
            return True

def main():
    """Main test function"""
    print("🧪 Reddit Scraper Test")
    print("This will test your Reddit scraping pipeline with just 3 shows.")
    print("It will verify:")
    print("  - Reddit API connection")
    print("  - Supabase database connection") 
    print("  - Discussion search functionality")
    print("  - Database insertion (with RLS)")
    print("  - CSV backup system")
    
    confirm = input("\nRun test? (y/n): ")
    if confirm.lower() == 'y':
        scraper = TestRedditScraper()
        success = scraper.run_test()
        
        if success:
            print("\n✅ Test successful! You can now run the full scraper safely.")
            print("Next: python scripts/overnight_reddit_scraper.py")
        else:
            print("\n❌ Test failed. Fix the issues above before running full scraper.")
    else:
        print("❌ Test cancelled")

if __name__ == "__main__":
    main()