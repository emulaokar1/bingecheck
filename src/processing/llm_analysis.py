#!/usr/bin/env python3
"""
LLM Analysis Pipeline for TV Show Analysis
Adds qualitative insights to peak analysis using OpenAI GPT-4
Save as: src/processing/llm_analysis.py
"""

import os
import json
import pandas as pd
import time
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import logging
from datetime import datetime
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TVShowLLMAnalyzer:
    def __init__(self):
        load_dotenv()
        
        # Initialize OpenAI
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        # Cost tracking
        self.total_tokens_used = 0
        self.total_cost = 0.0
        self.api_calls_made = 0
        
        # Rate limiting
        self.max_api_calls = int(os.getenv("MAX_API_CALLS", "9"))
        self.calls_per_minute = 3  # Conservative rate limiting
        self.last_call_time = 0
        
        # Create output directory
        Path("data/analysis").mkdir(parents=True, exist_ok=True)
        
    def load_peak_analysis(self):
        """Load peak analysis results"""
        analysis_file = Path("data/analysis/peak_analysis_results.json")
        
        if not analysis_file.exists():
            logger.error(f"❌ Peak analysis file not found: {analysis_file}")
            return None
        
        with open(analysis_file, 'r') as f:
            peak_analysis = json.load(f)
        
        logger.info(f"✅ Loaded peak analysis for {len(peak_analysis)} shows")
        return peak_analysis
    
    def load_reddit_discussions(self):
        """Load Reddit discussions from CSV"""
        reddit_file = Path("data/processed/reddit_discussions.csv")
        
        if not reddit_file.exists():
            logger.error(f"❌ Reddit discussions file not found: {reddit_file}")
            return None
        
        discussions_df = pd.read_csv(reddit_file)
        logger.info(f"✅ Loaded {len(discussions_df)} Reddit discussions")
        return discussions_df
    
    def filter_reddit_posts(self, show_id, keywords, discussions_df, max_posts=10):
        """Filter Reddit posts for specific show and keywords"""
        
        # Get posts for this show
        show_posts = discussions_df[discussions_df['show_id'] == show_id].copy()
        
        if len(show_posts) == 0:
            return []
        
        # Filter by keywords
        filtered_posts = []
        
        for _, post in show_posts.iterrows():
            # Handle NaN/float content safely
            title = str(post.get('title', '')) if pd.notna(post.get('title')) else ''
            content = str(post.get('content', '')) if pd.notna(post.get('content')) else ''
            
            combined_content = title + ' ' + content
            content_lower = combined_content.lower()
            
            # Check if any keyword matches
            if any(keyword.lower() in content_lower for keyword in keywords):
                filtered_posts.append({
                    'title': title,
                    'content': content,
                    'score': post.get('score', 0),
                    'subreddit': str(post.get('subreddit', '')) if pd.notna(post.get('subreddit')) else '',
                    'created_utc': str(post.get('created_utc', '')) if pd.notna(post.get('created_utc')) else ''
                })
        
        # Sort by score and take top posts
        filtered_posts.sort(key=lambda x: x['score'], reverse=True)
        return filtered_posts[:max_posts]
    
    def rate_limit_check(self):
        """Ensure we respect rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time
        
        # Wait at least 5 seconds between calls for testing (can increase for production)
        min_delay = 5
        if time_since_last < min_delay:
            sleep_time = min_delay - time_since_last
            logger.info(f"⏱️  Rate limiting: sleeping {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()
    
    def track_usage(self, response):
        """Track token usage and costs"""
        if hasattr(response, 'usage'):
            tokens = response.usage.total_tokens
            self.total_tokens_used += tokens
            
            # GPT-4o pricing: $2.50 per 1M input tokens, $10 per 1M output tokens
            # Approximate cost (assuming 80% input, 20% output)
            cost = (tokens * 0.8 * 2.50 + tokens * 0.2 * 10) / 1_000_000
            self.total_cost += cost
            
            self.api_calls_made += 1
            
            logger.info(f"💰 API call {self.api_calls_made}: {tokens} tokens, ~${cost:.4f} (Total: ${self.total_cost:.2f})")
    
    def get_system_prompt(self, prompt_type, show_index):
        """Get varied system prompts based on type and rotation"""
        
        system_prompts = [
            "You write concise TV recommendations for a website. Be informative and natural, avoiding clichés.",
            
            "You help viewers make informed decisions about TV shows. Write clear, conversational recommendations.",
            
            "You summarize fan opinions about TV shows for a recommendation website. Be direct and helpful.",
            
            "You analyze TV show discussions to help viewers. Write naturally without marketing speak.",
            
            "You extract insights from fan discussions for TV viewers. Keep it conversational and genuine."
        ]
        
        # Use with show index rotation
        return system_prompts[show_index % len(system_prompts)]

    def find_early_improvement_point(self, season_analysis):
        """Find when a show first shows sustained improvement using season-level data"""
        if not season_analysis or len(season_analysis) < 2:
            return None
        
        try:
            # Use season 1 as baseline
            season_1 = next((s for s in season_analysis if s.get('season') == 1), None)
            if not season_1:
                return None
            
            baseline_rating = season_1.get('avg_rating', 0)
            improvement_threshold = baseline_rating + 0.2  # More conservative for season-level data
            
            # Look for first season that shows clear improvement (but not past season 3)
            for season_data in season_analysis:
                season_num = season_data.get('season', 1)
                season_rating = season_data.get('avg_rating', 0)
                
                # Skip season 1 (it's our baseline)
                if season_num == 1:
                    continue
                
                # Don't recommend anything past season 3
                if season_num > 3:
                    break
                
                # If this season shows improvement, recommend the middle of it
                if season_rating >= improvement_threshold:
                    episode_count = season_data.get('episode_count', 10)
                    middle_episode = max(1, episode_count // 2)
                    
                    return {
                        'season': season_num,
                        'episode': middle_episode,
                        'rating': season_rating,
                        'improvement_from_s1': season_rating - baseline_rating
                    }
            
            # Fallback: if no improvement found, recommend end of season 1
            if season_1:
                episode_count = season_1.get('episode_count', 10)
                return {
                    'season': 1,
                    'episode': max(1, episode_count * 3 // 4),  # 3/4 through season 1
                    'rating': baseline_rating,
                    'improvement_from_s1': 0
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️  Error finding improvement point: {e}")
            return None
    
    def llm_analyze_when_gets_good(self, show_title, season_analysis, reddit_posts, show_index=0):
        """Analyze when a show gets good using early improvement trajectory and Reddit posts"""
        
        if not reddit_posts:
            return None
        
        # Check API call limit
        if self.api_calls_made >= self.max_api_calls:
            logger.warning(f"⚠️  Reached API call limit ({self.max_api_calls})")
            return None
        
        self.rate_limit_check()
        
        # Prepare Reddit content
        reddit_content = ""
        for post in reddit_posts[:5]:  # Top 5 posts
            reddit_content += f"Title: {post['title']}\n"
            reddit_content += f"Content: {post['content'][:300]}...\n"
            reddit_content += f"Score: {post['score']}\n\n"
        
        # Find early improvement point
        improvement_point = None
        if season_analysis:
            improvement_point = self.find_early_improvement_point(season_analysis)
        
        # Create context about improvement timing (if found)
        improvement_context = ""
        if improvement_point:
            season = improvement_point.get('season', 1)
            episode = improvement_point.get('episode', 1)
            rating = improvement_point.get('rating', 0)
            improvement = improvement_point.get('improvement_from_s1', 0)
            
            if improvement > 0:
                improvement_context = f"\n\nNote: Rating data shows the show begins to improve around Season {season}, Episode {episode} (rating: {rating:.1f}, +{improvement:.1f} from Season 1)."
            else:
                improvement_context = f"\n\nNote: Rating data suggests steady quality from Season {season}, Episode {episode} onward (rating: {rating:.1f})."
        
        prompt = f"""Based on these Reddit discussions about "{show_title}":

{reddit_content}

When do viewers typically become hooked on this show? Identify the specific episode or point where fans say it clicks for them and briefly explain why. 

If no clear point can be found, respond with something like "This is one of the rare shows that hooks you from the start."

{improvement_context}

Write exactly 1 sentence for website visitors deciding whether to continue watching. You will be generating many of these, so vary your wording."""
        
        try:
            system_prompt = self.get_system_prompt("when_gets_good", show_index)
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=120,
                temperature=0.5  # Higher for more variation
            )
            
            self.track_usage(response)
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"❌ Error in LLM analysis for {show_title}: {e}")
            return None
    
    def llm_analyze_ending_sentiment(self, show_title, reddit_posts, show_index=0):
        """Analyze ending sentiment using LLM"""
        
        if not reddit_posts:
            return None
        
        if self.api_calls_made >= self.max_api_calls:
            logger.warning(f"⚠️  Reached API call limit ({self.max_api_calls})")
            return None
        
        self.rate_limit_check()
        
        # Prepare finale discussions
        reddit_content = ""
        for post in reddit_posts[:5]:
            reddit_content += f"Title: {post['title']}\n"
            reddit_content += f"Content: {post['content'][:300]}...\n"
            reddit_content += f"Score: {post['score']}\n\n"
        
        prompt = f"""Based on these Reddit discussions about the ending of "{show_title}":

{reddit_content}

Write a casual, concise, and honest summary of how the ending was received. Sound like you're telling a friend whether the finale was worth it or not. 

Write exactly 2 sentences that help website visitors set expectations. Make sure to vary sentence structure and wording, but don't write more than 2 sentences."""
        
        try:
            system_prompt = self.get_system_prompt("ending_sentiment", show_index)
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=120,
                temperature=0.5  # Higher for more variation
            )
            
            self.track_usage(response)
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"❌ Error in ending analysis for {show_title}: {e}")
            return None
    
    def llm_analyze_best_run(self, show_title, best_run, reddit_posts, show_index=0):
        """Analyze why a quality run is special using episode ranges"""
        
        if not best_run:
            return None
        
        if self.api_calls_made >= self.max_api_calls:
            logger.warning(f"⚠️  Reached API call limit ({self.max_api_calls})")
            return None
        
        self.rate_limit_check()
        
        # Build episode range description
        start_season = best_run.get('start_season')
        start_episode = best_run.get('start_episode') 
        end_season = best_run.get('end_season')
        end_episode = best_run.get('end_episode')
        avg_rating = best_run.get('avg_rating', 0)
        
        if start_season == end_season:
            range_desc = f"Season {start_season}, Episodes {start_episode}-{end_episode}"
        else:
            range_desc = f"Season {start_season} Episode {start_episode} through Season {end_season} Episode {end_episode}"
        
        # Include some Reddit context if available
        reddit_context = ""
        if reddit_posts and len(reddit_posts) > 0:
            reddit_context = f"\n\nFan discussions mention this period positively."
        
        prompt = f"""Episodes {range_desc} from "{show_title}" represent the show's peak quality period.

Based on fan discussions:
{reddit_context}

Briefly explain what happens in these episodes/arc in a way that would excite someone deciding what to watch. Be specific. Avoid phrases like "in these episodes" - just say what happens.

Write exactly 2 sentences and using varying wording."""        
        try:
            system_prompt = self.get_system_prompt("best_run", show_index)
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=120,
                temperature=0.5  # Higher for more variation
            )
            
            self.track_usage(response)
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"❌ Error in run analysis for {show_title}: {e}")
            return None
        
    def llm_analyze_peak_episodes(self, show_title, peak_episodes, reddit_posts, show_index=0):
        """Analyze why specific peak episodes are special"""
        
        if not peak_episodes:
            return None

        if self.api_calls_made >= self.max_api_calls:
            logger.warning(f"⚠️  Reached API call limit ({self.max_api_calls})")
            return None

        self.rate_limit_check()

        # Build episode description
        if len(peak_episodes) == 1:
            ep = peak_episodes[0]
            episode_desc = f"Season {ep.get('season')}, Episode {ep.get('episode')} (Rating: {ep.get('rating')}/10)"
        else:
            episode_list = []
            for ep in peak_episodes:
                episode_list.append(f"S{ep.get('season')}E{ep.get('episode')} ({ep.get('rating')}/10)")
            episode_desc = f"Peak episodes: {', '.join(episode_list)}"

        # Include some Reddit context if available
        reddit_context = ""
        if reddit_posts and len(reddit_posts) > 0:
            reddit_context = f"\n\nFan discussions mention these episodes positively."

        prompt = f"""These episodes from "{show_title}" are highly rated: {episode_desc}

Based on fan discussions:
{reddit_context}

Briefly explain what happens in these episodes in a way that would excite someone deciding what to watch. Be specific.

Write exactly 2 sentences and using varying wording."""

        try:
            system_prompt = self.get_system_prompt("peak_episodes", show_index)
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=120,
                temperature=0.7
            )

            self.track_usage(response)
            return response.choices[0].message.content.strip()

        except Exception as e:
            logger.error(f"❌ Error in peak episode analysis for {show_title}: {e}")
            return None
    
    def llm_analyze_viewer_appeal(self, show_title, reddit_posts, show_index=0):
        """Analyze what viewers specifically like about the show"""
        
        if not reddit_posts:
            return None

        if self.api_calls_made >= self.max_api_calls:
            logger.warning(f"⚠️  Reached API call limit ({self.max_api_calls})")
            return None

        self.rate_limit_check()

        # Prepare Reddit content focused on positive aspects
        reddit_content = ""
        for post in reddit_posts[:5]:  # Top 5 posts
            reddit_content += f"Title: {post['title']}\n"
            reddit_content += f"Content: {post['content'][:300]}...\n"
            reddit_content += f"Score: {post['score']}\n\n"

        prompt = f"""Based on these Reddit discussions about "{show_title}":

{reddit_content}

What do viewers specifically like about this show? Focus on the core appeal - what keeps people watching and recommending it to others.

Write exactly 1 sentence that captures the main things viewers enjoy about the show. Vary your wording and focus on what makes it appealing."""

        try:
            system_prompt = self.get_system_prompt("viewer_appeal", show_index)
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=80,
                temperature=0.6
            )

            self.track_usage(response)
            return response.choices[0].message.content.strip()

        except Exception as e:
            logger.error(f"❌ Error in viewer appeal analysis for {show_title}: {e}")
            return None
        
    
    def analyze_single_show(self, show_analysis, discussions_df, show_index=0):
        """Analyze a single show with LLM insights"""
        
        show_title = show_analysis.get('show_title', 'Unknown')
        show_id = show_analysis.get('show_id')
        
        if not show_id:
            logger.warning(f"⚠️  No show_id found for {show_title}")
            return None
        
        logger.info(f"🎬 Analyzing {show_title} with LLM")
        
        # Create clean LLM-only analysis
        llm_analysis = {
            'show_id': show_id,
            'show_title': show_title
        }
        
        # 1. When it gets good analysis - focus on early engagement struggles
        when_good_keywords = [
            "gets good", "worth watching", "should I continue", "stick with it", 
            "slow start", "boring at first", "gets better after", "improves", 
            "first season slow", "when does it pick up", "worth pushing through",
            "skip first season", "starts slow", "hard to get into", "give it time",
            "patient with", "rough start", "first episodes boring", "when to stop watching",
            "does it get better", "struggle through", "payoff", "investment",
            # Expanded keywords to catch more general engagement discussions
            "first season", "season 1", "season one", "hooked", "addicted", "binge", 
            "marathon", "recommend", "worth it", "masterpiece", "amazing show",
            "started watching", "began watching", "episode 1", "pilot episode",
            "from the beginning", "early episodes", "opening episodes", "initial episodes"
        ]
        when_good_posts = self.filter_reddit_posts(show_id, when_good_keywords, discussions_df)
        
        # Get season analysis data for trajectory analysis
        season_analysis = show_analysis.get('season_analysis', [])
        
        if when_good_posts:
            when_good_analysis = self.llm_analyze_when_gets_good(
                show_title, 
                season_analysis,  # Pass season analysis for trajectory analysis
                when_good_posts,
                show_index
            )
            if when_good_analysis:
                llm_analysis['when_gets_good'] = when_good_analysis
        
        # 2. Ending sentiment analysis
        ending_keywords = ["finale", "ending", "conclusion", "final episode", "series finale", "last season", "disappointing", "satisfying"]
        ending_posts = self.filter_reddit_posts(show_id, ending_keywords, discussions_df)
        
        if ending_posts:
            ending_analysis = self.llm_analyze_ending_sentiment(show_title, ending_posts, show_index)
            if ending_analysis:
                llm_analysis['ending_sentiment'] = ending_analysis
        
        # 3. Viewer appeal analysis - what people like about the show
        appeal_keywords = [
            "love", "amazing", "brilliant", "excellent", "fantastic", "masterpiece", 
            "recommend", "favorite", "best show", "addicted", "hooked", "binge", 
            "incredible", "outstanding", "phenomenal", "perfect", "why I love",
            "what makes it great", "so good", "compelling", "gripping", "engaging"
        ]
        appeal_posts = self.filter_reddit_posts(show_id, appeal_keywords, discussions_df)
        
        if appeal_posts:
            appeal_analysis = self.llm_analyze_viewer_appeal(show_title, appeal_posts, show_index)
            if appeal_analysis:
                llm_analysis['viewer_appeal'] = appeal_analysis
        
        # 4. Best content analysis (adaptive - quality runs OR peak episodes)
        quality_runs = show_analysis.get('quality_runs')
        peak_episodes = show_analysis.get('peak_episodes')
        
        if quality_runs and isinstance(quality_runs, list) and len(quality_runs) > 0:
            # Show has quality runs - analyze the best run
            logger.info(f"📊 {show_title}: Using quality run analysis")
            best_run = quality_runs[0]  # Highest rated run
            if isinstance(best_run, dict):
                # Get some Reddit posts for additional context (but not required)
                run_keywords = ["best season", "peak", "masterpiece", f"season {best_run.get('start_season', '')}"]
                run_posts = self.filter_reddit_posts(show_id, run_keywords, discussions_df, max_posts=3)
                
                # Analyze the run
                run_analysis = self.llm_analyze_best_run(show_title, best_run, run_posts, show_index)
                if run_analysis:
                    llm_analysis['best_content'] = {
                        'type': 'quality_run',
                        'description': run_analysis
                    }
        
        elif peak_episodes and isinstance(peak_episodes, list) and len(peak_episodes) > 0:
            # No quality runs, but has peak episodes - highlight those
            logger.info(f"📊 {show_title}: Using peak episodes analysis")
            # Get Reddit posts for peak episodes context
            peak_keywords = ["best episode", "favorite episode", "peak", "masterpiece", "amazing episode"]
            peak_posts = self.filter_reddit_posts(show_id, peak_keywords, discussions_df, max_posts=3)
            
            # Analyze the peak episodes (take top 3)
            peak_analysis = self.llm_analyze_peak_episodes(show_title, peak_episodes[:3], peak_posts, show_index)
            if peak_analysis:
                llm_analysis['best_content'] = {
                    'type': 'peak_episodes',
                    'description': peak_analysis
                }
        
        else:
            logger.info(f"📊 {show_title}: No quality runs or peak episodes found")
        
        return llm_analysis
    
    def save_enhanced_analysis(self, enhanced_analysis):
        """Save enhanced analysis to file"""
        
        output_file = Path("data/analysis/llm_analysis_results.json")
        
        with open(output_file, 'w') as f:
            json.dump(enhanced_analysis, f, indent=2, default=str)
        
        logger.info(f"💾 Analysis results saved to {output_file}")
        
        # Save cost summary with history
        self.save_cost_summary()
        
        logger.info(f"💰 Cost summary: {self.api_calls_made} calls, {self.total_tokens_used} tokens, ${self.total_cost:.2f}")
    
    def save_analysis_only(self, enhanced_analysis):
        """Save analysis results without updating cost summary (for progress saves)"""
        output_file = Path("data/analysis/llm_analysis_results.json")
        
        with open(output_file, 'w') as f:
            json.dump(enhanced_analysis, f, indent=2, default=str)
    
    def save_cost_summary(self):
        """Save cost summary with history tracking"""
        cost_file = Path("data/analysis/llm_cost_summary.json")
        
        # Load existing cost history if it exists
        cost_history = {
            "total_all_time": {
                "total_api_calls": 0,
                "total_tokens_used": 0,
                "total_cost": 0.0,
                "last_updated": datetime.now().isoformat()
            },
            "runs": []
        }
        
        if cost_file.exists():
            try:
                with open(cost_file, 'r') as f:
                    existing_data = json.load(f)
                
                # Handle old format (single run) vs new format (history)
                if "runs" in existing_data:
                    cost_history = existing_data
                else:
                    # Convert old format to new format
                    cost_history["total_all_time"] = {
                        "total_api_calls": existing_data.get("total_api_calls", 0),
                        "total_tokens_used": existing_data.get("total_tokens_used", 0), 
                        "total_cost": existing_data.get("total_cost", 0.0),
                        "last_updated": existing_data.get("timestamp", datetime.now().isoformat())
                    }
                    cost_history["runs"] = [{
                        "run_date": existing_data.get("timestamp", datetime.now().isoformat()),
                        "api_calls": existing_data.get("total_api_calls", 0),
                        "tokens_used": existing_data.get("total_tokens_used", 0),
                        "cost": existing_data.get("total_cost", 0.0),
                        "shows_analyzed": "Historical"
                    }]
                    
            except Exception as e:
                logger.warning(f"⚠️  Could not load existing cost history: {e}")
        
        # Add current run to history
        current_run = {
            'run_date': datetime.now().isoformat(),
            'api_calls': self.api_calls_made,
            'tokens_used': self.total_tokens_used,
            'cost': round(self.total_cost, 2),
            'shows_analyzed': getattr(self, 'shows_analyzed', 'Unknown')
        }
        
        cost_history["runs"].append(current_run)
        
        # Update all-time totals
        cost_history["total_all_time"]["total_api_calls"] += self.api_calls_made
        cost_history["total_all_time"]["total_tokens_used"] += self.total_tokens_used
        cost_history["total_all_time"]["total_cost"] = round(
            cost_history["total_all_time"]["total_cost"] + self.total_cost, 2
        )
        cost_history["total_all_time"]["last_updated"] = datetime.now().isoformat()
        
        # Save updated history
        with open(cost_file, 'w') as f:
            json.dump(cost_history, f, indent=2)
        
        # Log summary
        all_time_cost = cost_history["total_all_time"]["total_cost"]
        all_time_calls = cost_history["total_all_time"]["total_api_calls"]
        logger.info(f"💰 All-time totals: {all_time_calls} calls, ${all_time_cost:.2f}")
        logger.info(f"📊 Run #{len(cost_history['runs'])}: +{self.api_calls_made} calls, +${self.total_cost:.2f}")
    
    def run_analysis_pipeline(self, max_shows=None):
        """Run the complete LLM analysis pipeline"""
        
        logger.info("🚀 Starting LLM analysis pipeline...")
        
        # Load data
        peak_analysis = self.load_peak_analysis()
        discussions_df = self.load_reddit_discussions()
        
        if not peak_analysis or discussions_df is None:
            logger.error("❌ Could not load required data")
            return
        
        # Limit shows for testing
        if max_shows:
            peak_analysis = peak_analysis[:max_shows]
            logger.info(f"🎯 Limiting to {max_shows} shows for testing")
        
        llm_analysis = []
        
        for show_index, show_analysis in enumerate(peak_analysis):
            try:
                llm_result = self.analyze_single_show(show_analysis, discussions_df, show_index)
                if llm_result:  # Only add if we got LLM insights
                    llm_analysis.append(llm_result)
                
                # Save progress periodically
                if len(llm_analysis) % 5 == 0:
                    # Don't save cost summary for partial progress
                    self.save_analysis_only(llm_analysis)
                    logger.info(f"💾 Progress saved: {len(llm_analysis)} shows analyzed")
                
            except Exception as e:
                logger.error(f"❌ Error analyzing {show_analysis.get('show_title', 'Unknown')}: {e}")
                continue
        
        # Track shows analyzed for cost summary
        self.shows_analyzed = len(llm_analysis)
        
        # Final save with cost summary
        self.save_enhanced_analysis(llm_analysis)
        
        logger.info(f"🎉 Analysis complete!")
        logger.info(f"📊 Analyzed {len(llm_analysis)} shows")
        logger.info(f"💰 Total cost: ${self.total_cost:.2f}")
        logger.info(f"🔄 API calls made: {self.api_calls_made}")
        
        return llm_analysis

def main():
    """Main function"""
    analyzer = TVShowLLMAnalyzer()
    
    # Run full analysis for all shows
    analyzer.run_analysis_pipeline()

if __name__ == "__main__":
    main()