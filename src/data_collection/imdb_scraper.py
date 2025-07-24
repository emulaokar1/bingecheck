#!/usr/bin/env python3
"""
IMDb Data Collection Script
Downloads and processes IMDb TSV datasets for TV shows
Save as: src/data_collection/imdb_scraper.py
"""

import os
import gzip
import pandas as pd
import requests
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IMDbScraper:
    def __init__(self):
        load_dotenv()
        
        # Try service role key first, fall back to anon key
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            supabase_key
        )
        
        # Try to authenticate if we have user credentials
        self._authenticate_if_possible()
        self.data_dir = Path("data/raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # IMDb dataset URLs
        self.imdb_urls = {
            'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
            'title_episodes': 'https://datasets.imdbws.com/title.episode.tsv.gz',
            'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
        }
    
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
    
    def download_imdb_datasets(self, force_download=False):
        """Download IMDb TSV files if they don't exist or if forced"""
        logger.info("📥 Downloading IMDb datasets...")
        
        for name, url in self.imdb_urls.items():
            file_path = self.data_dir / f"{name}.tsv.gz"
            
            if file_path.exists() and not force_download:
                logger.info(f"✅ {name} already exists, skipping download")
                continue
                
            logger.info(f"⬇️  Downloading {name}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(file_path, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            logger.info(f"✅ Downloaded {name}")
    
    def load_and_filter_shows(self, min_votes=1000, max_shows=500):
        """Load and filter TV shows from IMDb data"""
        logger.info("📊 Loading and filtering TV shows...")
        
        # Load title basics
        basics_file = self.data_dir / "title_basics.tsv.gz"
        logger.info("📖 Reading title basics...")
        
        # Read in chunks to handle large file
        chunk_size = 50000
        tv_shows = []
        
        with gzip.open(basics_file, 'rt', encoding='utf-8') as f:
            for chunk in tqdm(pd.read_csv(f, sep='\t', chunksize=chunk_size, low_memory=False), 
                            desc="Processing chunks"):
                # Filter for TV series and miniseries
                # First filter out \N values and non-numeric years
                chunk_filtered = chunk[
                    (chunk['titleType'].isin(['tvSeries', 'tvMiniSeries'])) &
                    (chunk['isAdult'] == 0) &  # No adult content
                    (chunk['startYear'] != '\\N') &  # Has start year
                    (chunk['startYear'].astype(str).str.isdigit())  # Valid year
                ]
                
                # Then apply year filter after ensuring we have valid integers
                if len(chunk_filtered) > 0:
                    tv_chunk = chunk_filtered[
                        pd.to_numeric(chunk_filtered['startYear'], errors='coerce') >= 1990
                    ]
                else:
                    tv_chunk = chunk_filtered
                
                if len(tv_chunk) > 0:
                    tv_shows.append(tv_chunk)
        
        if not tv_shows:
            raise ValueError("No TV shows found matching criteria")
        
        tv_shows_df = pd.concat(tv_shows, ignore_index=True)
        logger.info(f"📺 Found {len(tv_shows_df)} TV shows")
        
        # Load ratings to filter by popularity
        ratings_file = self.data_dir / "title_ratings.tsv.gz"
        logger.info("📖 Reading ratings...")
        
        with gzip.open(ratings_file, 'rt', encoding='utf-8') as f:
            ratings_df = pd.read_csv(f, sep='\t')
        
        # Merge with ratings and filter by vote count
        popular_shows = tv_shows_df.merge(ratings_df, on='tconst', how='inner')
        popular_shows = popular_shows[popular_shows['numVotes'] >= min_votes]
        
        # Sort by popularity and take top shows
        popular_shows = popular_shows.sort_values('numVotes', ascending=False).head(max_shows)
        
        logger.info(f"🎯 Selected {len(popular_shows)} popular shows")
        return popular_shows
    
    def load_episodes_for_shows(self, show_ids):
        """Load episode data for selected shows"""
        logger.info("📺 Loading episode data...")
        
        episodes_file = self.data_dir / "title_episodes.tsv.gz"
        
        with gzip.open(episodes_file, 'rt', encoding='utf-8') as f:
            episodes_df = pd.read_csv(f, sep='\t')
        
        # Filter episodes for our selected shows
        show_episodes = episodes_df[episodes_df['parentTconst'].isin(show_ids)]
        
        # Load ratings for episodes
        ratings_file = self.data_dir / "title_ratings.tsv.gz"
        with gzip.open(ratings_file, 'rt', encoding='utf-8') as f:
            ratings_df = pd.read_csv(f, sep='\t')
        
        # Merge episode data with ratings
        episodes_with_ratings = show_episodes.merge(
            ratings_df, 
            left_on='tconst', 
            right_on='tconst', 
            how='left'
        )
        
        logger.info(f"🎬 Found {len(episodes_with_ratings)} episodes")
        return episodes_with_ratings
    
    def clean_and_transform_data(self, shows_df, episodes_df):
        """Clean and transform data for database insertion"""
        logger.info("🧹 Cleaning and transforming data...")
        
        # Clean shows data
        shows_clean = shows_df.copy()
        shows_clean['genres'] = shows_clean['genres'].apply(
            lambda x: x.split(',') if x != '\\N' else []
        )
        shows_clean['endYear'] = shows_clean['endYear'].replace('\\N', None)
        shows_clean['runtimeMinutes'] = pd.to_numeric(
            shows_clean['runtimeMinutes'].replace('\\N', None), errors='coerce'
        )
        
        # Clean episodes data
        episodes_clean = episodes_df.copy()
        episodes_clean['seasonNumber'] = pd.to_numeric(
            episodes_clean['seasonNumber'].replace('\\N', None), errors='coerce'
        )
        episodes_clean['episodeNumber'] = pd.to_numeric(
            episodes_clean['episodeNumber'].replace('\\N', None), errors='coerce'
        )
        
        # Remove episodes with missing season/episode numbers
        episodes_clean = episodes_clean.dropna(subset=['seasonNumber', 'episodeNumber'])
        
        return shows_clean, episodes_clean
    
    def save_to_csv(self, shows_df, episodes_df):
        """Save cleaned data to CSV files as backup"""
        logger.info("💾 Saving data to CSV files...")
        
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save shows data
        shows_file = output_dir / "shows.csv"
        shows_df.to_csv(shows_file, index=False)
        logger.info(f"✅ Saved {len(shows_df)} shows to {shows_file}")
        
        # Save episodes data
        episodes_file = output_dir / "episodes.csv"
        episodes_df.to_csv(episodes_file, index=False)
        logger.info(f"✅ Saved {len(episodes_df)} episodes to {episodes_file}")
        
        return True

    def save_to_supabase(self, shows_df, episodes_df):
        """Save cleaned data to Supabase"""
        logger.info("💾 Saving data to Supabase...")
        
        # First save to CSV as backup
        self.save_to_csv(shows_df, episodes_df)
        
        # Prepare shows data for insertion
        shows_data = []
        for _, row in tqdm(shows_df.iterrows(), total=len(shows_df), desc="Preparing shows"):
            show_data = {
                'imdb_id': row['tconst'],
                'title': row['primaryTitle'],
                'original_title': row['originalTitle'] if row['originalTitle'] != row['primaryTitle'] else None,
                'start_year': int(row['startYear']) if pd.notna(row['startYear']) else None,
                'end_year': int(row['endYear']) if pd.notna(row['endYear']) else None,
                'runtime_minutes': int(row['runtimeMinutes']) if pd.notna(row['runtimeMinutes']) else None,
                'genres': row['genres'],
                'average_rating': float(row['averageRating']) if pd.notna(row['averageRating']) else None,
                'num_votes': int(row['numVotes']) if pd.notna(row['numVotes']) else None
            }
            shows_data.append(show_data)
        
        # Insert shows
        try:
            result = self.supabase.table('shows').upsert(shows_data).execute()
            logger.info(f"✅ Inserted {len(shows_data)} shows")
        except Exception as e:
            if "row-level security policy" in str(e):
                logger.error(f"❌ Database authentication error: {e}")
                logger.error("💡 Solutions:")
                logger.error("   1. Add SUPABASE_SERVICE_ROLE_KEY to your .env file")
                logger.error("   2. Add SUPABASE_USER_EMAIL and SUPABASE_USER_PASSWORD to .env")
                logger.error("   3. Disable RLS on 'shows' and 'episodes' tables in Supabase dashboard")
                logger.error("   4. Run: ALTER TABLE shows DISABLE ROW LEVEL SECURITY; in SQL editor")
                logger.info("✅ Data has been saved to CSV files in data/processed/ for manual import")
            else:
                logger.error(f"❌ Error inserting shows: {e}")
            return "csv_only"
        
        # Get show IDs for episode insertion
        show_id_map = {}
        shows_in_db = self.supabase.table('shows').select('id', 'imdb_id').execute()
        for show in shows_in_db.data:
            show_id_map[show['imdb_id']] = show['id']
        
        # Prepare episodes data
        episodes_data = []
        for _, row in tqdm(episodes_df.iterrows(), total=len(episodes_df), desc="Preparing episodes"):
            if row['parentTconst'] not in show_id_map:
                continue
                
            episode_data = {
                'show_id': show_id_map[row['parentTconst']],
                'imdb_id': row['tconst'],
                'season_number': int(row['seasonNumber']),
                'episode_number': int(row['episodeNumber']),
                'average_rating': float(row['averageRating']) if pd.notna(row['averageRating']) else None,
                'num_votes': int(row['numVotes']) if pd.notna(row['numVotes']) else None
            }
            episodes_data.append(episode_data)
        
        # Insert episodes in batches
        batch_size = 100
        for i in tqdm(range(0, len(episodes_data), batch_size), desc="Inserting episodes"):
            batch = episodes_data[i:i + batch_size]
            try:
                self.supabase.table('episodes').upsert(batch).execute()
            except Exception as e:
                logger.error(f"❌ Error inserting episode batch: {e}")
                continue
        
        logger.info(f"✅ Inserted {len(episodes_data)} episodes")
        return True
    
    def run_full_pipeline(self, max_shows=500, force_download=False):
        """Run the complete IMDb data collection pipeline"""
        logger.info("🚀 Starting IMDb data collection pipeline...")
        
        try:
            # Step 1: Download datasets
            self.download_imdb_datasets(force_download)
            
            # Step 2: Load and filter shows
            shows_df = self.load_and_filter_shows(max_shows=max_shows)
            
            # Step 3: Load episodes
            episodes_df = self.load_episodes_for_shows(shows_df['tconst'].tolist())
            
            # Step 4: Clean data
            shows_clean, episodes_clean = self.clean_and_transform_data(shows_df, episodes_df)
            
            # Step 5: Save to database
            success = self.save_to_supabase(shows_clean, episodes_clean)
            
            if success == True:
                logger.info("🎉 IMDb data collection completed successfully!")
                return True
            elif success == "csv_only":
                logger.info("🎉 IMDb data collection completed! Data saved to CSV files.")
                logger.info("💡 Import the CSV files manually when database permissions are fixed.")
                return True
            else:
                logger.error("❌ Failed to save data to database")
                return False
                
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False

def main():
    """Main function for testing"""
    scraper = IMDbScraper()
    
    # For testing, start with just 50 shows
    success = scraper.run_full_pipeline(max_shows=50, force_download=False)
    
    if success:
        print("\n✅ Test run completed! Check your Supabase database.")
        print("To run with more shows: scraper.run_full_pipeline(max_shows=500)")
    else:
        print("\n❌ Test run failed. Check the logs above.")

if __name__ == "__main__":
    main()