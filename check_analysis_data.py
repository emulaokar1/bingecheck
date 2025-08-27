#!/usr/bin/env python3

import os
from supabase import create_client, Client

from dotenv import load_dotenv
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(url, key)

try:
    print("Checking shows table structure and analysis data...")
    result = supabase.table('shows').select('*').eq('id', 7).execute()  # Breaking Bad
    
    if result.data:
        show = result.data[0]
        print(f"Show: {show.get('title')}")
        print("Available fields:")
        for key, value in show.items():
            if value is not None:
                print(f"  {key}: {value}")
    else:
        print("No show found")
        
except Exception as e:
    print(f"Error: {e}")