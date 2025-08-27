'use client'

import { useState, useRef, useEffect } from 'react';
import { Search } from 'lucide-react';
import { Show } from '../types';
import { searchShows } from '../data/mockData';

interface DropdownSearchBarProps {
  placeholder?: string;
  className?: string;
}

export const DropdownSearchBar = ({ placeholder = "Search for TV shows...", className = "" }: DropdownSearchBarProps) => {
  const [query, setQuery] = useState('');
  const [shows, setShows] = useState<Show[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSearch = async (searchQuery: string) => {
    if (!searchQuery.trim()) {
      setShows([]);
      setIsOpen(false);
      return;
    }

    setIsLoading(true);
    try {
      const results = await searchShows(searchQuery);
      setShows(results.slice(0, 6)); // Limit to 6 results for dropdown
      setIsOpen(true);
    } catch (error) {
      console.error('Error searching shows:', error);
      setShows([]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setQuery(value);
    
    // Simple debounce
    if (value.length > 2 || value.length === 0) {
      setTimeout(() => {
        if (inputRef.current?.value === value) {
          handleSearch(value);
        }
      }, 300);
    }
  };

  const handleShowClick = (showId: number) => {
    setIsOpen(false);
    setQuery('');
    window.location.href = `/shows/${showId}`;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (shows.length > 0) {
      handleShowClick(shows[0].id);
    }
  };

  return (
    <div ref={dropdownRef} className={`relative w-full max-w-md mx-auto ${className}`}>
      <form onSubmit={handleSubmit}>
        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400 h-4 w-4" />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleChange}
          placeholder={placeholder}
          className="w-full pl-10 h-10 text-base bg-slate-800 text-white placeholder-slate-400 border border-slate-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
        />
      </form>

      {/* Dropdown */}
      {isOpen && (
        <div className="absolute top-full left-0 right-0 z-50 mt-1 bg-slate-800 border border-slate-600 rounded-lg shadow-lg max-h-80 overflow-y-auto">
          {isLoading ? (
            <div className="p-4 text-center text-slate-400">
              Searching...
            </div>
          ) : shows.length > 0 ? (
            <ul className="py-2">
              {shows.map((show) => (
                <li key={show.id}>
                  <button
                    onClick={() => handleShowClick(show.id)}
                    className="w-full px-4 py-3 text-left hover:bg-slate-700 transition-colors focus:outline-none focus:bg-slate-700"
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-white font-medium">{show.title}</p>
                        <p className="text-slate-400 text-sm">
                          {show.start_year}{show.end_year ? `-${show.end_year}` : ''} • {show.genres.join(', ')}
                        </p>
                      </div>
                      <div className="flex items-center text-yellow-400">
                        <span className="text-sm">★ {show.average_rating}</span>
                      </div>
                    </div>
                  </button>
                </li>
              ))}
            </ul>
          ) : query.length > 2 ? (
            <div className="p-4 text-center text-slate-400">
              No shows found
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
};