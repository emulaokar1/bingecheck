#!/usr/bin/env python3

import os
from supabase import create_client, Client

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")  # Use service role to bypass RLS

if not url or not key:
    print("❌ Missing environment variables")
    exit(1)

supabase: Client = create_client(url, key)

try:
    print("Testing shows table query...")
    result = supabase.table('shows').select('id, title, start_year, average_rating').limit(5).execute()
    if result.data:
        print(f"✅ Found {len(result.data)} shows:")
        for show in result.data:
            print(f"  - ID: {show.get('id')}, Title: {show.get('title')}, Year: {show.get('start_year')}, Rating: {show.get('average_rating')}")
    else:
        print("❌ No shows found in database")
        
    print("\nTesting show_analysis table...")
    analysis_result = supabase.table('show_analysis').select('*').limit(3).execute()
    if analysis_result.data:
        print(f"✅ Found {len(analysis_result.data)} analyses")
        for analysis in analysis_result.data:
            print(f"  - Show ID: {analysis.get('show_id')}, Title: {analysis.get('show_title')}")
    else:
        print("❌ No analyses found - table may not exist or be empty")
        
except Exception as e:
    print(f"❌ Error: {e}")