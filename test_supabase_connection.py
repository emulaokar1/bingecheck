#!/usr/bin/env python3
"""
Test Supabase connection and authentication
"""

import os
from dotenv import load_dotenv
from supabase import create_client

def test_connection():
    load_dotenv()
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    print(f"URL: {url}")
    print(f"Key starts with: {key[:20]}..." if key else "No key found")
    
    supabase = create_client(url, key)
    
    # Test basic connection
    try:
        # Try to read from shows table
        result = supabase.table('shows').select('id').limit(1).execute()
        print("✅ Can read from shows table")
        print(f"Result: {result}")
    except Exception as e:
        print(f"❌ Cannot read from shows table: {e}")
    
    # Test insert (will likely fail due to RLS)
    try:
        test_data = {
            'imdb_id': 'test123',
            'title': 'Test Show',
            'start_year': 2023
        }
        result = supabase.table('shows').insert(test_data).execute()
        print("✅ Can insert into shows table")
        print(f"Insert result: {result}")
    except Exception as e:
        print(f"❌ Cannot insert into shows table: {e}")
        if "row-level security" in str(e):
            print("💡 This is the RLS policy issue we need to fix")

if __name__ == "__main__":
    test_connection()