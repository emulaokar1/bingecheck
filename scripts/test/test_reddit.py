#!/usr/bin/env python3
"""
Test script to verify Reddit API connection
Save as: scripts/test_reddit.py
"""

import os
import praw
from dotenv import load_dotenv

def test_reddit_connection():
    # Load environment variables
    load_dotenv()
    
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")
    
    if not all([client_id, client_secret, user_agent]):
        print("❌ Error: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT must be set in .env file")
        return False
    
    try:
        # Create Reddit instance
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        
        print("✅ Reddit client created successfully")
        
        # Test API access with a simple request
        # We'll search for a popular TV show subreddit
        subreddit = reddit.subreddit("television")
        print(f"✅ Successfully accessed r/television")
        print(f"📊 Subreddit has {subreddit.subscribers:,} subscribers")
        
        # Test searching for TV show discussions
        print("\n🔍 Testing search functionality...")
        search_results = list(subreddit.search("Breaking Bad", limit=3))
        print(f"✅ Found {len(search_results)} posts about Breaking Bad")
        
        for i, post in enumerate(search_results, 1):
            print(f"   {i}. {post.title[:60]}...")
            print(f"      Score: {post.score}, Comments: {post.num_comments}")
        
        # Test rate limiting info
        print(f"\n📈 API Rate Limit Info:")
        print(f"   Requests remaining: {reddit.auth.limits.get('remaining', 'Unknown')}")
        print(f"   Reset time: {reddit.auth.limits.get('reset_timestamp', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to Reddit API: {e}")
        print(f"   Check your credentials and make sure they're correct")
        return False

def test_specific_show_search():
    """Test searching for specific TV show discussions"""
    load_dotenv()
    
    try:
        reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT")
        )
        
        print("\n🎬 Testing TV show specific searches...")
        
        # Test different search strategies
        test_shows = ["Breaking Bad", "Game of Thrones", "The Office"]
        
        for show in test_shows:
            print(f"\n--- Searching for '{show}' ---")
            
            # Search in general TV subreddits
            tv_sub = reddit.subreddit("television")
            posts = list(tv_sub.search(show, limit=2))
            print(f"r/television: {len(posts)} posts found")
            
            # Try to find show-specific subreddit
            try:
                show_name = show.lower().replace(" ", "")
                show_sub = reddit.subreddit(show_name)
                print(f"r/{show_name}: {show_sub.subscribers:,} subscribers")
            except:
                print(f"r/{show_name}: Subreddit not found or private")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in show search test: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Reddit API connection...")
    
    # Basic connection test
    success = test_reddit_connection()
    
    if success:
        # Advanced functionality test
        test_specific_show_search()
        print("\n🎉 All Reddit API tests passed! Ready to collect TV show discussions.")
    else:
        print("\n💥 Reddit API tests failed. Please check your configuration.")
        print("\nTroubleshooting tips:")
        print("1. Make sure you selected 'script' as the app type")
        print("2. Double-check your client ID and secret")
        print("3. Ensure your user agent follows the format: AppName/Version by YourUsername")