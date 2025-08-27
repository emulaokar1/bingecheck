'use client'

import { useState, useCallback, useEffect } from 'react';
import { SearchBar } from './SearchBar';
import { ShowCard } from './ShowCard';
import { ShowCardGrid } from './LoadingStates';
import { mockShows, searchShows, getShows } from '../data/mockData';
import { Show } from '../types';

export const SearchPage = () => {
  const [shows, setShows] = useState<Show[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    // Load initial shows
    loadInitialShows();
  }, []);

  const loadInitialShows = async () => {
    setIsLoading(true);
    try {
      const allShows = await getShows();
      setShows(allShows.slice(0, 6)); // Limit to 6 shows
    } catch (error) {
      console.error('Error loading shows:', error);
      setShows(mockShows.slice(0, 6)); // Fallback to mock data, limited to 6
    } finally {
      setIsLoading(false);
    }
  };

  const handleSearch = useCallback(async (query: string) => {
    if (!mounted) return; // Prevent search during hydration
    
    setIsLoading(true);
    try {
      const results = await searchShows(query);
      // If no search query (empty), show only 6 popular shows
      if (!query.trim()) {
        setShows(results.slice(0, 6));
      } else {
        setShows(results); // Show all search results
      }
    } catch (error) {
      console.error('Error searching shows:', error);
      setShows([]); // Clear results on error
    } finally {
      setIsLoading(false);
    }
  }, [mounted]);

  if (!mounted) {
    return (
      <div className="min-h-screen bg-background">
        <main className="container mx-auto px-4 py-8">
          <div className="text-center mb-8">
            <h1 className="text-4xl font-bold mb-6">BingeCheck</h1>
            <div className="max-w-md mx-auto mb-8">
              <SearchBar onSearch={() => {}} placeholder="Search for TV shows..." variant="light" />
            </div>
          </div>
          <div>
            <h2 className="text-2xl font-semibold mb-6">Loading...</h2>
            <ShowCardGrid />
          </div>
        </main>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Main Content */}
      <main className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold mb-6">BingeCheck</h1>
          <div className="max-w-md mx-auto mb-8">
            <SearchBar onSearch={handleSearch} placeholder="Search for TV shows..." variant="light" />
          </div>
        </div>
        {/* Results Section */}
        <div>
          <h2 className="text-2xl font-semibold mb-6">
            {isLoading ? 'Loading...' : 'Popular Shows'}
          </h2>
          
          {isLoading ? (
            <ShowCardGrid />
          ) : (
            <>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                {shows && shows.length > 0 && shows.map((show) => (
                  <ShowCard key={show.id} show={show} />
                ))}
              </div>
              
              {!isLoading && (!shows || shows.length === 0) && (
                <div className="text-center py-12">
                  <p className="text-lg text-muted-foreground">
                    No shows found. Try a different search term.
                  </p>
                </div>
              )}
            </>
          )}
        </div>
      </main>
    </div>
  );
};

