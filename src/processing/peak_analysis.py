#!/usr/bin/env python3
"""
TV Show Peak Analysis Engine
Analyzes IMDb episode data to find peaks, quality runs, and declines
Save as: src/processing/peak_analysis.py
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
import logging
from scipy import stats
from scipy.signal import find_peaks
import matplotlib.pyplot as plt
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TVPeakAnalyzer:
    def __init__(self):
        load_dotenv()
        
        # Initialize Supabase
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            supabase_key
        )
        
        # Create output directory
        Path("data/analysis").mkdir(parents=True, exist_ok=True)
        
    def load_show_episode_data(self):
        """Load shows and episodes from Supabase"""
        try:
            # Load shows
            shows_result = self.supabase.table('shows').select('*').execute()
            shows_df = pd.DataFrame(shows_result.data)
            
            # Load ALL episodes using pagination
            all_episodes = []
            page_size = 1000
            offset = 0
            
            while True:
                episodes_result = self.supabase.table('episodes').select('*').range(offset, offset + page_size - 1).execute()
                if not episodes_result.data:
                    break
                all_episodes.extend(episodes_result.data)
                if len(episodes_result.data) < page_size:
                    break
                offset += page_size
                logger.info(f"📥 Loaded {len(all_episodes)} episodes so far...")
            
            episodes_df = pd.DataFrame(all_episodes)
            
            logger.info(f"✅ Loaded {len(shows_df)} shows and {len(episodes_df)} episodes")
            return shows_df, episodes_df
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return None, None
    
    def analyze_single_show(self, show_id, show_title, episodes_df):
        """Analyze peaks and quality runs for a single show"""
        
        # Filter episodes for this show
        show_episodes = episodes_df[
            (episodes_df['show_id'] == show_id) & 
            (episodes_df['average_rating'].notna())
        ].copy()
        
        if len(show_episodes) == 0:
            logger.warning(f"⚠️  No rated episodes found for {show_title}")
            return None
        
        # Sort by season and episode
        show_episodes = show_episodes.sort_values(['season_number', 'episode_number'])
        
        logger.info(f"🎬 Analyzing {show_title} ({len(show_episodes)} episodes)")
        
        ratings = show_episodes['average_rating'].values
        episode_numbers = range(1, len(ratings) + 1)
        
        # Calculate basic statistics
        show_avg = np.mean(ratings)
        show_std = np.std(ratings)
        
        analysis = {
            'show_id': show_id,
            'show_title': show_title,
            'total_episodes': len(show_episodes),
            'average_rating': round(show_avg, 2),
            'rating_std': round(show_std, 2),
            'peak_episodes': self.find_peak_episodes(show_episodes, ratings, show_avg, show_std),
            'quality_runs': self.find_quality_runs(show_episodes, ratings, show_avg),
            'decline_analysis': self.analyze_quality_decline(show_episodes, ratings),
            'season_analysis': self.analyze_by_season(show_episodes),
            'recommendation': self.generate_recommendation(show_episodes, ratings, show_avg)
        }
        
        return analysis
    
    def find_peak_episodes(self, episodes_df, ratings, show_avg, show_std):
        """Find individual peak episodes using statistical methods"""
        
        # Method 1: Episodes significantly above average
        high_threshold = show_avg + (show_std * 0.8)
        peak_threshold = show_avg + (show_std * 1.2)
        
        peak_episodes = []
        
        for idx, episode in episodes_df.iterrows():
            rating = episode['average_rating']
            
            if rating >= peak_threshold:
                peak_episodes.append({
                    'episode_id': episode['id'],
                    'season': int(episode['season_number']),
                    'episode': int(episode['episode_number']),
                    'rating': round(rating, 1),
                    'above_avg': round(rating - show_avg, 1),
                    'significance': 'peak' if rating >= peak_threshold else 'high'
                })
        
        # Sort by rating (highest first)
        peak_episodes.sort(key=lambda x: x['rating'], reverse=True)
        
        return peak_episodes[:10]  # Top 10 peak episodes
    
    def find_quality_runs(self, episodes_df, ratings, show_avg):
        """Find consecutive episodes of high quality"""
        
        # Define quality threshold (above average + some buffer)
        quality_threshold = show_avg + 0.3
        min_run_length = 3
        
        quality_runs = []
        current_run = []
        
        episodes_list = episodes_df.to_dict('records')
        
        for i, episode in enumerate(episodes_list):
            rating = episode['average_rating']
            
            if rating >= quality_threshold:
                # Add to current run
                current_run.append({
                    'season': int(episode['season_number']),
                    'episode': int(episode['episode_number']),
                    'rating': round(rating, 1)
                })
            else:
                # End current run if it's long enough
                if len(current_run) >= min_run_length:
                    avg_rating = np.mean([ep['rating'] for ep in current_run])
                    quality_runs.append({
                        'start_season': current_run[0]['season'],
                        'start_episode': current_run[0]['episode'],
                        'end_season': current_run[-1]['season'],
                        'end_episode': current_run[-1]['episode'],
                        'length': len(current_run),
                        'avg_rating': round(avg_rating, 1),
                        'episodes': current_run.copy()
                    })
                current_run = []
        
        # Check final run
        if len(current_run) >= min_run_length:
            avg_rating = np.mean([ep['rating'] for ep in current_run])
            quality_runs.append({
                'start_season': current_run[0]['season'],
                'start_episode': current_run[0]['episode'],
                'end_season': current_run[-1]['season'],
                'end_episode': current_run[-1]['episode'],
                'length': len(current_run),
                'avg_rating': round(avg_rating, 1),
                'episodes': current_run.copy()
            })
        
        # Sort by average rating (best runs first)
        quality_runs.sort(key=lambda x: x['avg_rating'], reverse=True)
        
        return quality_runs
    
    def analyze_quality_decline(self, episodes_df, ratings):
        """Analyze if/when the show's quality declines"""
        
        if len(ratings) < 10:  # Need enough episodes for trend analysis
            return None
        
        # Calculate moving averages to smooth out noise
        window_size = min(5, len(ratings) // 3)
        moving_avg = pd.Series(ratings).rolling(window=window_size, center=True).mean()
        
        # Find the peak of the moving average
        peak_idx = np.nanargmax(moving_avg)
        peak_rating = moving_avg.iloc[peak_idx]
        
        # Look for significant decline after peak
        decline_threshold = peak_rating - 0.5
        decline_episodes = []
        
        for i in range(peak_idx + 1, len(moving_avg)):
            if not pd.isna(moving_avg.iloc[i]) and moving_avg.iloc[i] <= decline_threshold:
                episode = episodes_df.iloc[i]
                decline_episodes.append({
                    'season': int(episode['season_number']),
                    'episode': int(episode['episode_number']),
                    'rating': round(ratings[i], 1),
                    'moving_avg': round(moving_avg.iloc[i], 1)
                })
                break  # Found first significant decline
        
        if decline_episodes:
            return {
                'peak_episode_index': int(peak_idx),
                'peak_rating': round(peak_rating, 1),
                'decline_starts': decline_episodes[0],
                'decline_severity': round(peak_rating - decline_episodes[0]['moving_avg'], 1)
            }
        
        return None
    
    def analyze_by_season(self, episodes_df):
        """Analyze average rating by season"""
        
        season_stats = []
        
        for season_num in sorted(episodes_df['season_number'].unique()):
            season_episodes = episodes_df[episodes_df['season_number'] == season_num]
            season_ratings = season_episodes['average_rating'].dropna()
            
            if len(season_ratings) > 0:
                season_stats.append({
                    'season': int(season_num),
                    'episode_count': len(season_episodes),
                    'avg_rating': round(season_ratings.mean(), 2),
                    'min_rating': round(season_ratings.min(), 1),
                    'max_rating': round(season_ratings.max(), 1),
                    'rating_range': round(season_ratings.max() - season_ratings.min(), 1)
                })
        
        return season_stats
    
    def generate_recommendation(self, episodes_df, ratings, show_avg):
        """Generate viewing recommendations based on analysis"""
        
        recommendations = []
        
        # Check if show starts slow
        first_few = ratings[:min(5, len(ratings))]
        if len(first_few) >= 3 and np.mean(first_few) < show_avg - 0.3:
            # Find when it gets better
            for i in range(3, min(10, len(ratings))):
                if ratings[i] > show_avg:
                    episode = episodes_df.iloc[i]
                    recommendations.append({
                        'type': 'slow_start',
                        'message': f"Starts slow but improves around S{int(episode['season_number'])}E{int(episode['episode_number'])}",
                        'season': int(episode['season_number']),
                        'episode': int(episode['episode_number'])
                    })
                    break
        
        # Check for strong finish
        last_few = ratings[-min(5, len(ratings)):]
        if len(last_few) >= 3 and np.mean(last_few) > show_avg + 0.3:
            recommendations.append({
                'type': 'strong_finish',
                'message': "Strong finish - stick with it to the end"
            })
        
        return recommendations
    
    def save_analysis(self, all_analysis):
        """Save analysis results to JSON file"""
        
        output_file = Path("data/analysis/peak_analysis_results.json")
        
        with open(output_file, 'w') as f:
            json.dump(all_analysis, f, indent=2, default=str)
        
        logger.info(f"💾 Analysis saved to {output_file}")
    
    def create_summary_report(self, all_analysis):
        """Create a summary report of all shows"""
        
        summary = {
            'total_shows_analyzed': len(all_analysis),
            'shows_with_peaks': len([a for a in all_analysis if a and a.get('peak_episodes')]),
            'shows_with_quality_runs': len([a for a in all_analysis if a and a.get('quality_runs')]),
            'shows_with_decline': len([a for a in all_analysis if a and a.get('decline_analysis')]),
            'top_rated_shows': sorted(
                [a for a in all_analysis if a], 
                key=lambda x: x['average_rating'], 
                reverse=True
            )[:10]
        }
        
        return summary
    
    def run_full_analysis(self):
        """Run peak analysis for all shows"""
        
        logger.info("🚀 Starting TV show peak analysis...")
        
        # Load data
        shows_df, episodes_df = self.load_show_episode_data()
        if shows_df is None:
            return
        
        all_analysis = []
        
        # Analyze each show
        for _, show in shows_df.iterrows():
            try:
                analysis = self.analyze_single_show(
                    show['id'], 
                    show['title'], 
                    episodes_df
                )
                
                if analysis:
                    all_analysis.append(analysis)
                    
                    # Print quick summary
                    peaks = len(analysis.get('peak_episodes', []))
                    runs = len(analysis.get('quality_runs', []))
                    logger.info(f"  📊 {analysis['show_title']}: {peaks} peaks, {runs} quality runs")
                    
            except Exception as e:
                logger.error(f"❌ Error analyzing {show['title']}: {e}")
                continue
        
        # Save results
        self.save_analysis(all_analysis)
        
        # Create summary
        summary = self.create_summary_report(all_analysis)
        
        logger.info(f"\n🎉 Analysis complete!")
        logger.info(f"📊 Analyzed {summary['total_shows_analyzed']} shows")
        logger.info(f"🏔️  Shows with peaks: {summary['shows_with_peaks']}")
        logger.info(f"🏃 Shows with quality runs: {summary['shows_with_quality_runs']}")
        logger.info(f"📉 Shows with decline: {summary['shows_with_decline']}")
        
        return all_analysis, summary

def main():
    """Main function"""
    analyzer = TVPeakAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main()
