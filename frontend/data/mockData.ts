import { Show, Episode } from '@/types';
import { getAllShows, searchShows as apiSearchShows, getShowById } from '@/lib/api';

export const mockShows: Show[] = [
  {
    id: 1,
    imdb_id: "tt0903747",
    title: "Breaking Bad",
    start_year: 2008,
    end_year: 2013,
    genres: ["Crime", "Drama", "Thriller"],
    average_rating: 9.5,
    num_votes: 1500000,
    poster_url: "https://images.unsplash.com/photo-1461749280684-dccba630e2f6?w=300&h=450&fit=crop",
    when_gets_good: "Season 1, Episode 5 - 'Gray Matter'",
    ending_sentiment: "praised",
    viewer_appeal: "Masterful character development and incredible tension building",
    total_discussions: 427,
    best_content_description: "Walter White's transformation from mild-mannered teacher to ruthless drug kingpin is perfectly executed with top-tier writing and acting."
  },
  {
    id: 2,
    imdb_id: "tt0306414",
    title: "The Wire",
    start_year: 2002,
    end_year: 2008,
    genres: ["Crime", "Drama"],
    average_rating: 9.3,
    num_votes: 350000,
    poster_url: "https://images.unsplash.com/photo-1487058792275-0ad4aaf24ca7?w=300&h=450&fit=crop",
    when_gets_good: "Season 1, Episode 3 - 'The Buys'",
    ending_sentiment: "praised",
    viewer_appeal: "Realistic portrayal of Baltimore's institutions and complex social issues",
    total_discussions: 312,
    best_content_description: "A deep dive into Baltimore's drug trade, politics, and institutions with incredible realism and social commentary."
  },
  {
    id: 3,
    imdb_id: "tt0944947",
    title: "Game of Thrones",
    start_year: 2011,
    end_year: 2019,
    genres: ["Adventure", "Drama", "Fantasy"],
    average_rating: 9.2,
    num_votes: 2000000,
    poster_url: "https://images.unsplash.com/photo-1526374965328-7f61d4dc18c5?w=300&h=450&fit=crop",
    when_gets_good: "Season 1, Episode 6 - 'A Golden Crown'",
    ending_sentiment: "disappointing",
    viewer_appeal: "Epic fantasy with complex politics and stunning production values",
    total_discussions: 892,
    best_content_description: "While the early seasons offer incredible world-building and character development, the final seasons disappointed many fans with rushed storytelling."
  },
  {
    id: 4,
    imdb_id: "tt3110726",
    title: "Better Call Saul",
    start_year: 2015,
    end_year: 2022,
    genres: ["Crime", "Drama"],
    average_rating: 8.9,
    num_votes: 450000,
    poster_url: "https://images.unsplash.com/photo-1498050108023-c5249f4df085?w=300&h=450&fit=crop",
    when_gets_good: "Season 1, Episode 2 - 'Mijo'",
    ending_sentiment: "praised",
    viewer_appeal: "Excellent character study with perfect pacing and cinematography",
    total_discussions: 256,
    best_content_description: "A masterful prequel that stands on its own with incredible character development and visual storytelling."
  },
  {
    id: 5,
    imdb_id: "tt0141842",
    title: "The Sopranos",
    start_year: 1999,
    end_year: 2007,
    genres: ["Crime", "Drama"],
    average_rating: 9.2,
    num_votes: 400000,
    poster_url: "https://images.unsplash.com/photo-1483058712412-4245e9b90334?w=300&h=450&fit=crop",
    when_gets_good: "Season 1, Episode 5 - 'College'",
    ending_sentiment: "mixed",
    viewer_appeal: "Groundbreaking character study of a mob boss balancing family life",
    total_discussions: 389,
    best_content_description: "Revolutionary TV drama that paved the way for modern prestige television with complex anti-hero storytelling."
  },
  {
    id: 6,
    imdb_id: "tt0386676",
    title: "The Office",
    start_year: 2005,
    end_year: 2013,
    genres: ["Comedy"],
    average_rating: 8.8,
    num_votes: 600000,
    poster_url: "https://images.unsplash.com/photo-1500673922987-e212871fec22?w=300&h=450&fit=crop",
    when_gets_good: "Season 2, Episode 1 - 'The Dundies'",
    ending_sentiment: "mixed",
    viewer_appeal: "Hilarious mockumentary style with heart and memorable characters",
    total_discussions: 567,
    best_content_description: "A workplace comedy that balances humor with genuine emotional moments, though quality varies in later seasons."
  }
];

export const mockEpisodes: Episode[] = [
  // Breaking Bad episodes
  { id: 1, show_id: 1, season: 1, episode: 1, rating: 8.2, title: "Pilot" },
  { id: 2, show_id: 1, season: 1, episode: 2, rating: 8.4, title: "Cat's in the Bag..." },
  { id: 3, show_id: 1, season: 1, episode: 3, rating: 8.6, title: "...And the Bag's in the River" },
  { id: 4, show_id: 1, season: 1, episode: 4, rating: 8.8, title: "Cancer Man" },
  { id: 5, show_id: 1, season: 1, episode: 5, rating: 9.0, title: "Gray Matter" },
  { id: 6, show_id: 1, season: 1, episode: 6, rating: 9.1, title: "Crazy Handful of Nothin'" },
  { id: 7, show_id: 1, season: 1, episode: 7, rating: 9.2, title: "A No-Rough-Stuff-Type Deal" },
  
  // Season 2
  { id: 8, show_id: 1, season: 2, episode: 1, rating: 8.8, title: "Seven Thirty-Seven" },
  { id: 9, show_id: 1, season: 2, episode: 2, rating: 9.0, title: "Grilled" },
  { id: 10, show_id: 1, season: 2, episode: 3, rating: 8.9, title: "Bit by a Dead Bee" },
  { id: 11, show_id: 1, season: 2, episode: 4, rating: 9.1, title: "Down" },
  { id: 12, show_id: 1, season: 2, episode: 5, rating: 9.2, title: "Breakage" },
  { id: 13, show_id: 1, season: 2, episode: 6, rating: 9.3, title: "Peekaboo" },
  { id: 14, show_id: 1, season: 2, episode: 7, rating: 9.4, title: "Negro y Azul" },
  { id: 15, show_id: 1, season: 2, episode: 8, rating: 9.5, title: "Better Call Saul" },
  { id: 16, show_id: 1, season: 2, episode: 9, rating: 9.6, title: "4 Days Out" },
  { id: 17, show_id: 1, season: 2, episode: 10, rating: 9.7, title: "Over" },
  { id: 18, show_id: 1, season: 2, episode: 11, rating: 9.8, title: "Mandala" },
  { id: 19, show_id: 1, season: 2, episode: 12, rating: 9.8, title: "Phoenix" },
  { id: 20, show_id: 1, season: 2, episode: 13, rating: 9.9, title: "ABQ" },
];

// Legacy functions for backward compatibility
export const getShowByIdLegacy = (id: number): Show | undefined => {
  return mockShows.find(show => show.id === id);
};

export const getEpisodesByShowId = (showId: number): Episode[] => {
  return mockEpisodes.filter(episode => episode.show_id === showId);
};

// New async functions that use Supabase
export const getShows = async (): Promise<Show[]> => {
  try {
    const shows = await getAllShows();
    return shows;
  } catch (error) {
    console.error('Error fetching shows:', error);
    return mockShows; // Fallback to mock data
  }
};

export const searchShows = async (query: string): Promise<Show[]> => {
  try {
    if (!query.trim()) {
      return await getAllShows();
    }
    const shows = await apiSearchShows(query);
    return shows;
  } catch (error) {
    console.error('Error searching shows:', error);
    // Fallback to mock data search
    const lowerQuery = query.toLowerCase();
    return mockShows.filter(show => 
      show.title.toLowerCase().includes(lowerQuery) ||
      show.genres.some(genre => genre.toLowerCase().includes(lowerQuery))
    );
  }
};

export const getShowData = async (id: number): Promise<Show | null> => {
  try {
    const show = await getShowById(id);
    return show;
  } catch (error) {
    console.error('Error fetching show:', error);
    return getShowByIdLegacy(id) || null;
  }
};