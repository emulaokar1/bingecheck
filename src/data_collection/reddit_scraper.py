#!/usr/bin/env python3
"""
Overnight Reddit Data Collection Script
Slowly collects Reddit discussions about your TV shows
Save as: scripts/overnight_reddit_scraper.py
"""

import os
import time
import praw
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
import logging
import json

# Set up logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/reddit_scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OvernightRedditScraper:
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
        
        # Try to authenticate if we have user credentials
        self._authenticate_if_possible()
        
        # Create directories
        Path("data/reddit_cache").mkdir(parents=True, exist_ok=True)
        Path("data/processed").mkdir(parents=True, exist_ok=True)
        
        # Rate limiting
        self.request_count = 0
        self.start_time = time.time()
        self.delay_between_requests = 1.5  # Be conservative with rate limits
        
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
        
    def rate_limit_check(self):
        """Ensure we don't exceed Reddit's rate limits"""
        self.request_count += 1
        
        # Log progress every 50 requests
        if self.request_count % 50 == 0:
            elapsed = time.time() - self.start_time
            rate = self.request_count / (elapsed / 60)  # requests per minute
            logger.info(f"📊 Progress: {self.request_count} requests, {rate:.1f}/min")
        
        # Sleep to respect rate limits
        time.sleep(self.delay_between_requests)
    
    def get_shows_from_database(self):
        """Get all shows from database"""
        try:
            result = self.supabase.table('shows').select('*').execute()
            logger.info(f"📺 Loaded {len(result.data)} shows from database")
            return result.data
        except Exception as e:
            logger.error(f"❌ Failed to load shows: {e}")
            return []
    
    def search_show_discussions(self, show_title, show_id, limit_per_search=25):
        """Search for discussions about a specific show"""
        discussions = []
        
        # Search strategies
        search_terms = [
            show_title,
            f"{show_title} discussion",
            f"{show_title} episode",
            f"{show_title} finale",
            f"{show_title} season"
        ]
        
        # Target subreddits
        subreddits = ['television', 'netflix', 'hbo', 'AskReddit']
        
        # Try show-specific subreddit
        show_sub_name = show_title.lower().replace(' ', '').replace(':', '').replace('-', '')
        try:
            show_subreddit = self.reddit.subreddit(show_sub_name)
            # Test if it exists and is active
            if show_subreddit.subscribers > 100:
                subreddits.insert(0, show_sub_name)
                logger.info(f"🎯 Found show subreddit: r/{show_sub_name}")
        except:
            pass
        
        for subreddit_name in subreddits:
            for search_term in search_terms:
                try:
                    self.rate_limit_check()
                    
                    subreddit = self.reddit.subreddit(subreddit_name)
                    
                    # Search posts
                    posts = subreddit.search(
                        search_term, 
                        sort='top', 
                        time_filter='all',
                        limit=limit_per_search
                    )
                    
                    for post in posts:
                        # Basic relevance check
                        if show_title.lower() in post.title.lower() or show_title.lower() in post.selftext.lower():
                            discussion = {
                                'show_id': show_id,
                                'reddit_id': post.id,
                                'title': post.title,
                                'content': post.selftext,
                                'score': post.score,
                                'upvote_ratio': post.upvote_ratio,
                                'num_comments': post.num_comments,
                                'created_utc': datetime.fromtimestamp(post.created_utc),
                                'subreddit': post.subreddit.display_name,
                                'author': str(post.author) if post.author else '[deleted]',
                                'url': post.url,
                                'is_discussion': any(word in post.title.lower() for word in ['discussion', 'episode', 'finale', 'thoughts'])
                            }
                            discussions.append(discussion)
                    
                    logger.info(f"🔍 Found {len(list(posts))} posts for '{search_term}' in r/{subreddit_name}")
                    
                except Exception as e:
                    logger.warning(f"⚠️  Error searching r/{subreddit_name} for '{search_term}': {e}")
                    continue
        
        # Remove duplicates based on reddit_id
        unique_discussions = {}
        for disc in discussions:
            if disc['reddit_id'] not in unique_discussions:
                unique_discussions[disc['reddit_id']] = disc
        
        return list(unique_discussions.values())
    
    def save_progress(self, show_data, discussions_data):
        """Save progress to both CSV and Supabase (robust like IMDb scraper)"""
        try:
            # Always save to CSV as backup first (like IMDb scraper)
            discussions_df = pd.DataFrame(discussions_data)
            if len(discussions_df) > 0:
                csv_path = Path("data/processed/reddit_discussions.csv")
                discussions_df.to_csv(csv_path, index=False)
                logger.info(f"💾 Saved {len(discussions_df)} discussions to CSV backup")
            
            # Try to save to Supabase
            try:
                if discussions_data:
                    # Prepare data for Supabase (convert datetime to string)
                    supabase_data = []
                    for disc in discussions_data:
                        supabase_disc = disc.copy()
                        # Handle datetime conversion properly
                        if isinstance(disc['created_utc'], datetime):
                            supabase_disc['created_utc'] = disc['created_utc'].isoformat()
                        supabase_data.append(supabase_disc)
                    
                    # Insert in batches to avoid timeouts
                    batch_size = 50
                    successful_inserts = 0
                    
                    for i in range(0, len(supabase_data), batch_size):
                        batch = supabase_data[i:i + batch_size]
                        try:
                            result = self.supabase.table('reddit_posts').upsert(batch).execute()
                            successful_inserts += len(batch)
                        except Exception as batch_error:
                            logger.warning(f"⚠️  Batch insert failed: {batch_error}")
                            continue
                    
                    logger.info(f"✅ Saved {successful_inserts}/{len(discussions_data)} discussions to Supabase")
                    
            except Exception as db_error:
                # Handle RLS errors gracefully (like IMDb scraper)
                if "row-level security policy" in str(db_error):
                    logger.warning(f"⚠️  Database authentication error: {db_error}")
                    logger.info("💡 Fix: Add SUPABASE_SERVICE_ROLE_KEY to .env or disable RLS")
                else:
                    logger.warning(f"⚠️  Supabase insert failed: {db_error}")
                logger.info("📄 Data preserved in CSV backup")
            
            # Save progress log
            progress = {
                'timestamp': datetime.now().isoformat(),
                'completed_shows': len(show_data),
                'total_discussions': len(discussions_data),
                'request_count': self.request_count
            }
            
            with open("data/reddit_scraping_progress.json", "w") as f:
                json.dump(progress, f, indent=2)
                
        except Exception as e:
            logger.error(f"❌ Error saving progress: {e}")
    
    def run_overnight_collection(self):
        """Run the complete overnight collection"""
        logger.info("🌙 Starting overnight Reddit data collection...")
        logger.info(f"⏰ Started at: {datetime.now()}")
        
        shows = self.get_shows_from_database()
        if not shows:
            logger.error("❌ No shows found in database")
            return
        
        all_discussions = []
        completed_shows = []
        
        for i, show in enumerate(shows, 1):
            logger.info(f"\n📺 Processing show {i}/{len(shows)}: {show['title']}")
            
            try:
                discussions = self.search_show_discussions(show['title'], show['id'])
                
                if discussions:
                    all_discussions.extend(discussions)
                    logger.info(f"✅ Found {len(discussions)} discussions for {show['title']}")
                else:
                    logger.info(f"ℹ️  No discussions found for {show['title']}")
                
                completed_shows.append(show)
                
                # Save progress every 10 shows
                if i % 10 == 0:
                    self.save_progress(completed_shows, all_discussions)
                    logger.info(f"💾 Progress saved: {i}/{len(shows)} shows completed")
                
                # Longer break between shows to be respectful
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Error processing {show['title']}: {e}")
                continue
        
        # Final save
        self.save_progress(completed_shows, all_discussions)
        
        # Summary
        elapsed = time.time() - self.start_time
        logger.info(f"\n🎉 Overnight collection complete!")
        logger.info(f"⏰ Total time: {elapsed/3600:.1f} hours")
        logger.info(f"📊 Total shows processed: {len(completed_shows)}")
        logger.info(f"💬 Total discussions found: {len(all_discussions)}")
        logger.info(f"🔄 Total API requests: {self.request_count}")
        logger.info(f"📁 Data saved to: data/processed/reddit_discussions.csv")

def main():
    """Main function"""
    scraper = OvernightRedditScraper()
    
    print("🌙 Reddit Data Collection Script")
    print("This will collect Reddit discussions about your TV shows.")
    print("The script will:")
    print("  - Process all 50 shows in your database")
    print("  - Search multiple subreddits for each show")
    print("  - Respect Reddit's rate limits (slow but steady)")
    print("  - Save progress every 10 shows")
    print("  - Create detailed logs in data/reddit_scraping.log")
    print("\n⚠️  Mac Sleep Warning:")
    print("  - Run with: caffeinate -i python scripts/overnight_reddit_scraper.py")
    print("  - Or adjust your Energy Saver settings")
    print("  - Script saves progress every 10 shows, so you can resume if interrupted")
    
    confirm = input("\nStart collection? (y/n): ")
    if confirm.lower() == 'y':
        scraper.run_overnight_collection()
    else:
        print("❌ Cancelled")

if __name__ == "__main__":
    main()