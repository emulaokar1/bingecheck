-- Temporarily disable Row Level Security for data import
-- Run this in your Supabase SQL Editor before running the IMDb scraper

-- Disable RLS on shows table
ALTER TABLE shows DISABLE ROW LEVEL SECURITY;

-- Disable RLS on episodes table  
ALTER TABLE episodes DISABLE ROW LEVEL SECURITY;

-- Optional: Re-enable after import is complete
-- ALTER TABLE shows ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE episodes ENABLE ROW LEVEL SECURITY;