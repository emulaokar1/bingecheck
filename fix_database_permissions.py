#!/usr/bin/env python3
"""
Fix database permissions by disabling RLS temporarily
"""

import os
from dotenv import load_dotenv
from supabase import create_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def disable_rls():
    """Disable Row Level Security on tables for data import"""
    load_dotenv()
    
    # We need to use a service role key or admin access for this
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not service_key:
        logger.error("❌ SUPABASE_SERVICE_ROLE_KEY required to disable RLS")
        logger.error("💡 Get this from your Supabase project settings > API")
        return False
    
    supabase = create_client(os.getenv("SUPABASE_URL"), service_key)
    
    try:
        # Disable RLS on shows table
        supabase.postgrest.rpc('exec_sql', {
            'sql': 'ALTER TABLE shows DISABLE ROW LEVEL SECURITY;'
        }).execute()
        
        # Disable RLS on episodes table
        supabase.postgrest.rpc('exec_sql', {
            'sql': 'ALTER TABLE episodes DISABLE ROW LEVEL SECURITY;'
        }).execute()
        
        logger.info("✅ Successfully disabled RLS on shows and episodes tables")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to disable RLS: {e}")
        return False

if __name__ == "__main__":
    disable_rls()