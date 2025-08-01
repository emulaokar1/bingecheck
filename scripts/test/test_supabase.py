#!/usr/bin/env python3
"""
Test script to verify Supabase connection and database setup
Save as: scripts/test_supabase.py
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client

def test_supabase_connection():
    # Load environment variables
    load_dotenv()
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        print("❌ Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
        return False
    
    try:
        # Create Supabase client
        supabase: Client = create_client(url, key)
        print("✅ Supabase client created successfully")
        
        # Test connection by querying shows table
        result = supabase.table('shows').select("count", count="exact").execute()
        print(f"✅ Database connection successful")
        print(f"📊 Shows table has {result.count} records")
        
        # Test all tables exist
        tables_to_test = ['shows', 'episodes', 'reddit_posts', 'reddit_comments', 'sentiment_scores', 'show_statistics']
        
        for table in tables_to_test:
            try:
                supabase.table(table).select("count", count="exact").execute()
                print(f"✅ Table '{table}' exists and is accessible")
            except Exception as e:
                print(f"❌ Error accessing table '{table}': {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to Supabase: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Supabase connection...")
    success = test_supabase_connection()
    
    if success:
        print("\n🎉 All tests passed! Your Supabase setup is ready.")
    else:
        print("\n💥 Tests failed. Please check your configuration.")