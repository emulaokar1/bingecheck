'use client'

import { useState } from 'react';
import { Search } from 'lucide-react';

interface SimpleSearchBarProps {
  onSearch: (query: string) => void;
  placeholder?: string;
  className?: string;
}

export const SimpleSearchBar = ({ onSearch, placeholder = "Search for TV shows...", className = "" }: SimpleSearchBarProps) => {
  const [query, setQuery] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch(query);
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setQuery(value);
    
    // Simple debounce without useEffect
    if (value.length > 2 || value.length === 0) {
      setTimeout(() => {
        if (e.target.value === value) { // Only search if value hasn't changed
          onSearch(value);
        }
      }, 300);
    }
  };

  return (
    <form onSubmit={handleSubmit} className={`relative w-full max-w-md mx-auto ${className}`}>
      <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400 h-4 w-4" />
      <input
        type="text"
        value={query}
        onChange={handleChange}
        placeholder={placeholder}
        className="w-full pl-10 h-10 text-base bg-slate-800 text-white placeholder-slate-400 border border-slate-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
      />
    </form>
  );
};