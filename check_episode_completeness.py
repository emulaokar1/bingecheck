#!/usr/bin/env python3
"""
Script to check episode ratings completeness for each show in Supabase.
Analyzes the episodes table to identify shows with missing or incomplete episode data.
"""

import os
from supabase import create_client, Client
from collections import defaultdict
import json

def main():
    # Supabase configuration
    url = 'https://xlenxmzrwkufqmorvhjq.supabase.co'
    service_role_key = 'sb_secret_q29wFwCVb_2fR45qprmSUQ_vY0mc6cM'
    
    try:
        supabase: Client = create_client(url, service_role_key)
        
        print("🔍 Checking episode ratings completeness...\n")
        
        # Get all shows with their basic info
        shows_result = supabase.table('shows').select('id, title, start_year, end_year').execute()
        shows = {show['id']: show for show in shows_result.data}
        
        # Get all episodes (handle pagination due to 1000 record limit)
        all_episodes = []
        page_size = 1000
        offset = 0
        
        print("📥 Fetching all episodes (this may take a moment)...")
        while True:
            episodes_result = supabase.table('episodes').select('*').range(offset, offset + page_size - 1).execute()
            if not episodes_result.data:
                break
            all_episodes.extend(episodes_result.data)
            offset += page_size
            print(f"   Fetched {len(all_episodes)} episodes so far...")
            
            # Break if we got less than a full page
            if len(episodes_result.data) < page_size:
                break
        
        episodes = all_episodes
        
        print(f"📊 Found {len(shows)} shows and {len(episodes)} episodes\n")
        
        # Group episodes by show
        episodes_by_show = defaultdict(list)
        for episode in episodes:
            episodes_by_show[episode['show_id']].append(episode)
        
        # Analyze completeness
        report = {
            'summary': {
                'total_shows': len(shows),
                'shows_with_episodes': 0,
                'shows_without_episodes': 0,
                'total_episodes': len(episodes),
                'episodes_with_ratings': 0,
                'episodes_missing_ratings': 0
            },
            'shows': []
        }
        
        print("📋 Episode Completeness Report")
        print("=" * 50)
        
        for show_id, show_info in shows.items():
            show_episodes = episodes_by_show.get(show_id, [])
            
            if not show_episodes:
                report['summary']['shows_without_episodes'] += 1
                show_report = {
                    'show_id': show_id,
                    'title': show_info['title'],
                    'year_range': f"{show_info['start_year']}-{show_info.get('end_year', 'ongoing')}",
                    'total_episodes': 0,
                    'episodes_with_ratings': 0,
                    'episodes_missing_ratings': 0,
                    'completeness_percentage': 0,
                    'status': 'NO_EPISODES',
                    'missing_seasons': [],
                    'gaps_in_episodes': []
                }
                print(f"❌ {show_info['title']} - NO EPISODES FOUND")
            else:
                report['summary']['shows_with_episodes'] += 1
                
                # Count episodes with/without ratings
                episodes_with_ratings = [ep for ep in show_episodes if ep.get('average_rating') is not None]
                episodes_missing_ratings = [ep for ep in show_episodes if ep.get('average_rating') is None]
                
                report['summary']['episodes_with_ratings'] += len(episodes_with_ratings)
                report['summary']['episodes_missing_ratings'] += len(episodes_missing_ratings)
                
                # Analyze season/episode gaps
                seasons = defaultdict(list)
                for ep in show_episodes:
                    seasons[ep['season_number']].append(ep['episode_number'])
                
                # Check for gaps in episodes within seasons
                gaps_in_episodes = []
                for season_num, episode_nums in seasons.items():
                    episode_nums.sort()
                    for i in range(1, len(episode_nums)):
                        if episode_nums[i] - episode_nums[i-1] > 1:
                            gaps_in_episodes.append({
                                'season': season_num,
                                'gap': f"Episodes {episode_nums[i-1]} to {episode_nums[i]}"
                            })
                
                completeness_percentage = (len(episodes_with_ratings) / len(show_episodes)) * 100 if show_episodes else 0
                
                # Determine status
                if completeness_percentage == 100:
                    status = 'COMPLETE'
                    status_icon = "✅"
                elif completeness_percentage >= 80:
                    status = 'MOSTLY_COMPLETE'
                    status_icon = "⚠️"
                elif completeness_percentage >= 50:
                    status = 'PARTIAL'
                    status_icon = "🔶"
                else:
                    status = 'INCOMPLETE'
                    status_icon = "❌"
                
                show_report = {
                    'show_id': show_id,
                    'title': show_info['title'],
                    'year_range': f"{show_info['start_year']}-{show_info.get('end_year', 'ongoing')}",
                    'total_episodes': len(show_episodes),
                    'episodes_with_ratings': len(episodes_with_ratings),
                    'episodes_missing_ratings': len(episodes_missing_ratings),
                    'completeness_percentage': round(completeness_percentage, 1),
                    'status': status,
                    'season_count': len(seasons),
                    'seasons': dict(seasons),
                    'gaps_in_episodes': gaps_in_episodes
                }
                
                print(f"{status_icon} {show_info['title']} - {len(show_episodes)} episodes, {completeness_percentage:.1f}% complete")
                if gaps_in_episodes:
                    print(f"    📍 Episode gaps found: {len(gaps_in_episodes)} gaps")
                if episodes_missing_ratings:
                    print(f"    🚫 Missing ratings: {len(episodes_missing_ratings)} episodes")
            
            report['shows'].append(show_report)
        
        print("\n" + "=" * 50)
        print("📈 SUMMARY STATISTICS")
        print("=" * 50)
        print(f"Total Shows: {report['summary']['total_shows']}")
        print(f"Shows with Episodes: {report['summary']['shows_with_episodes']}")
        print(f"Shows without Episodes: {report['summary']['shows_without_episodes']}")
        print(f"Total Episodes: {report['summary']['total_episodes']}")
        print(f"Episodes with Ratings: {report['summary']['episodes_with_ratings']}")
        print(f"Episodes missing Ratings: {report['summary']['episodes_missing_ratings']}")
        
        if report['summary']['total_episodes'] > 0:
            overall_completeness = (report['summary']['episodes_with_ratings'] / report['summary']['total_episodes']) * 100
            print(f"Overall Completeness: {overall_completeness:.1f}%")
        
        # Save detailed report
        with open('/Users/emulaokar/Desktop/bingecheck/episode_completeness_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n💾 Detailed report saved to: episode_completeness_report.json")
        
        # Show top incomplete shows
        incomplete_shows = [s for s in report['shows'] if s['status'] in ['INCOMPLETE', 'PARTIAL']]
        if incomplete_shows:
            print(f"\n🔍 TOP SHOWS NEEDING ATTENTION:")
            incomplete_shows.sort(key=lambda x: x['completeness_percentage'])
            for show in incomplete_shows[:10]:
                print(f"  • {show['title']}: {show['completeness_percentage']}% complete ({show['episodes_missing_ratings']} missing ratings)")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()