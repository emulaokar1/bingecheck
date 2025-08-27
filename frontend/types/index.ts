export interface Show {
  id: number;
  imdb_id: string;
  title: string;
  start_year: number;
  end_year?: number;
  runtime_minutes?: number;
  genres: string[];
  average_rating: number;
  num_votes: number;
  poster_url?: string;
  when_gets_good?: string;
  ending_sentiment?: string;
  viewer_appeal?: string;
  total_discussions?: number;
  best_content_description?: string;
}

export interface ShowAnalysis {
  show_id: number;
  show_title: string;
  when_gets_good: string;
  ending_sentiment: string;
  best_content: {
    type: string;
    description: string;
  };
  viewer_appeal: string;
}

export interface Episode {
  id: number;
  show_id: number;
  season: number;
  episode: number;
  rating: number;
  title?: string;
}

export type EndingSentiment = 'praised' | 'mixed' | 'disappointing' | null;