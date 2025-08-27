import os
import json
import time
import praw
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from openai import OpenAI
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

load_dotenv()

class RedditEnricher:
    def __init__(self):
        # Initialize clients
        self.supabase = create_client(
            os.environ.get("SUPABASE_URL"),
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        )
        
        self.openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        self.reddit = praw.Reddit(
            client_id=os.environ.get("REDDIT_CLIENT_ID"),
            client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
            user_agent="BingeCheck:v1.0"
        )
        
        # Cache for subreddit lookups
        self.subreddit_cache = {}
        
    def find_shows_needing_enrichment(self, min_discussions=50):
        """Find shows with fewer than X discussions"""
        logger.info(f"Finding specific shows needing enrichment...")
        
        # Only process LOTR
        target_shows = {
            50: "The Lord of the Rings: The Rings of Power"
        }
        
        shows = []
        for show_id, title in target_shows.items():
            # Get current discussion count
            result = self.supabase.table('show_statistics')\
                .select('total_discussions')\
                .eq('show_id', show_id)\
                .execute()
            
            discussion_count = result.data[0]['total_discussions'] if result.data else 0
            
            shows.append({
                'id': show_id,
                'title': title,
                'current_discussions': discussion_count
            })
        
        logger.info(f"Targeting {len(shows)} specific shows for enrichment")
        return shows

    def find_subreddit_with_gpt(self, show_title):
        """Hardcoded subreddit for LOTR"""
        
        if show_title == "The Lord of the Rings: The Rings of Power":
            subreddit = "LOTR_on_Prime"
            logger.info(f"✓ Using hardcoded subreddit for '{show_title}': r/{subreddit}")
            return subreddit
        
        logger.error(f"No hardcoded subreddit for '{show_title}'")
        return None

        def verify_subreddit(self, subreddit_name):
            """Check if a subreddit exists and is active"""
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                # Try to access subreddit info
                _ = subreddit.subscribers
                return True
            except:
                return False

    def scrape_subreddit_for_show(self, subreddit_name, show_id, show_title, limit=500):
        """Scrape posts from a specific subreddit for a show"""
        logger.info(f"Scraping r/{subreddit_name} for '{show_title}'...")
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            scraped_posts = []
            
            # Method 1: Get ALL top posts from the subreddit (not filtered by show title)
            logger.info("Getting top posts from all time...")
            for post in subreddit.top(time_filter='all', limit=300):
                scraped_posts.append(self.process_reddit_post(post, show_id))
                
            # Method 2: Get hot/recent posts
            logger.info("Getting recent hot posts...")
            for post in subreddit.hot(limit=200):
                scraped_posts.append(self.process_reddit_post(post, show_id))
            
            # Method 3: Search for discussion keywords (broader)
            discussion_terms = ["episode", "season", "discussion", "thoughts", "theory", "spoiler"]
            for term in discussion_terms:
                try:
                    for post in subreddit.search(term, limit=100, sort='top'):
                        scraped_posts.append(self.process_reddit_post(post, show_id))
                    time.sleep(1)
                except:
                    continue
            
            # Remove duplicates
            seen = set()
            unique_posts = []
            for post in scraped_posts:
                if post['reddit_id'] not in seen:
                    seen.add(post['reddit_id'])
                    unique_posts.append(post)
            
            logger.info(f"Found {len(unique_posts)} total posts")
            return unique_posts
            
        except Exception as e:
            logger.error(f"Error scraping r/{subreddit_name}: {e}")
            return []

    def is_relevant_post(self, post, keywords):
        """Check if a post is relevant to the show"""
        title_lower = post.title.lower()
        
        # Check if any keyword is in the title
        return any(keyword in title_lower for keyword in keywords)

    def process_reddit_post(self, post, show_id):
        """Convert PRAW post to database format"""
        return {
            'show_id': show_id,
            'reddit_id': post.id,
            'title': post.title[:500],  # Truncate to fit DB
            'content': post.selftext[:5000] if post.selftext else '',
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
            'subreddit': post.subreddit.display_name,
            'author': str(post.author) if post.author else '[deleted]',
            'url': f"https://reddit.com{post.permalink}",
            'is_discussion': 'discussion' in post.title.lower()
        }

    def save_posts_to_db(self, posts):
        """Save scraped posts to database"""
        if not posts:
            return
        
        # Insert in batches
        batch_size = 50
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            try:
                self.supabase.table('reddit_posts').insert(batch).execute()
                logger.info(f"Saved batch of {len(batch)} posts")
            except Exception as e:
                logger.error(f"Error saving batch: {e}")

    def rerun_llm_analysis(self, show_id, show_title):
        """Re-run LLM analysis for a show with new Reddit data"""
        logger.info(f"Re-running LLM analysis for '{show_title}'...")
        
        # Import the LLM analyzer
        from src.processing.llm_analysis import TVShowLLMAnalyzer
        
        analyzer = TVShowLLMAnalyzer()
        
        # Get Reddit discussions for this show
        reddit_result = self.supabase.table('reddit_posts')\
            .select('*')\
            .eq('show_id', show_id)\
            .execute()
        
        # Convert to DataFrame for compatibility
        discussions_df = pd.DataFrame(reddit_result.data)
        
        if len(discussions_df) == 0:
            logger.warning(f"No Reddit data found for show {show_id}")
            return None
        
        # LOAD THE ORIGINAL PEAK ANALYSIS DATA
        with open('data/analysis/peak_analysis_results.json', 'r') as f:
            peak_data = json.load(f)
        
        # Find this show's analysis
        show_analysis = None
        for show in peak_data:
            if show['show_id'] == show_id:
                show_analysis = show
                break
        
        if not show_analysis:
            logger.warning(f"No peak analysis found for show {show_id}")
            return None
        
        # Run analysis with proper data structure
        llm_result = analyzer.analyze_single_show(show_analysis, discussions_df)
        
        return llm_result

    def update_show_statistics(self, show_id, llm_analysis, new_discussion_count):
        """Update show_statistics with new analysis"""
        if not llm_analysis:
            return
        
        # Count ONLY relevant discussions (same as original filtering)
        reddit_result = self.supabase.table('reddit_posts')\
            .select('*')\
            .eq('show_id', show_id)\
            .execute()
        
        discussions_df = pd.DataFrame(reddit_result.data)
        
        # Use same keywords as original analysis
        all_keywords = [
            "gets good", "worth watching", "should I continue", "stick with it", 
            "slow start", "boring at first", "gets better after", "improves", 
            "first season slow", "when does it pick up", "finale", "ending", 
            "conclusion", "final episode", "series finale", "last season", 
            "disappointing", "satisfying", "best season", "peak", "masterpiece", 
            "best episode", "favorite episode", "amazing episode"
        ]
        
        # Count only posts matching keywords
        relevant_count = 0
        for _, post in discussions_df.iterrows():
            title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
            content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
            combined = (title + ' ' + content).lower()
            
            if any(keyword.lower() in combined for keyword in all_keywords):
                relevant_count += 1
        
        update_data = {
            'when_gets_good': llm_analysis.get('when_gets_good'),
            'ending_sentiment': llm_analysis.get('ending_sentiment'),
            'best_content_type': llm_analysis.get('best_content', {}).get('type'),
            'best_content_description': llm_analysis.get('best_content', {}).get('description'),
            'viewer_appeal': llm_analysis.get('viewer_appeal'),
            'total_discussions': relevant_count,  # Only filtered count
            'last_calculated': datetime.now().isoformat()
        }
        
        try:
            self.supabase.table('show_statistics')\
                .update(update_data)\
                .eq('show_id', show_id)\
                .execute()
            logger.info(f"✓ Updated statistics for show {show_id} with {relevant_count} relevant discussions")
        except Exception as e:
            logger.error(f"Error updating statistics: {e}")

    def enrich_show(self, show):
        """Complete enrichment process for one show"""
        show_id = show['id']
        show_title = show['title']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {show_title}")
        logger.info(f"Current discussions: {show['current_discussions']}")
        
        # Step 1: Find correct subreddit
        subreddit = self.find_subreddit_with_gpt(show_title)
        if not subreddit:
            logger.warning(f"Could not find subreddit for '{show_title}'")
            return
        
        # Step 2: Scrape subreddit
        new_posts = self.scrape_subreddit_for_show(subreddit, show_id, show_title)
        
        # Step 3: Save to database
        if new_posts:
            self.save_posts_to_db(new_posts)
            
            # Step 4: Re-run LLM analysis
            llm_analysis = self.rerun_llm_analysis(show_id, show_title)
            
            # Step 5: Update statistics
            if llm_analysis:
                # Get new total count
                count_result = self.supabase.table('reddit_posts')\
                    .select('id', count='exact')\
                    .eq('show_id', show_id)\
                    .execute()
                
                new_count = count_result.count
                self.update_show_statistics(show_id, llm_analysis, new_count)
                
                logger.info(f"✓ Enrichment complete! New discussion count: {new_count}")
            else:
                logger.warning("LLM analysis failed")
        else:
            logger.warning("No new posts found")

    def run_enrichment(self, min_discussions=50):
        """Run the complete enrichment process"""
        logger.info("Starting Reddit enrichment process...")
        
        # Find shows needing enrichment
        shows = self.find_shows_needing_enrichment(min_discussions)
        
        # Process each show
        for i, show in enumerate(shows):
            logger.info(f"\nProcessing {i+1}/{len(shows)}")
            try:
                self.enrich_show(show)
                time.sleep(5)  # Rate limiting between shows
            except Exception as e:
                logger.error(f"Error enriching {show['title']}: {e}")
                continue
        
        logger.info("\n✓ Enrichment process complete!")

def main():
    enricher = RedditEnricher()
    enricher.run_enrichment(min_discussions=50)

if __name__ == "__main__":
    main()