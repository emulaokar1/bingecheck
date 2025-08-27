'use client'

import { useState, useEffect } from 'react';
import { Search } from 'lucide-react';

interface SearchBarProps {
  onSearch: (query: string) => void;
  placeholder?: string;
  className?: string;
  variant?: 'light' | 'dark';
}

export const SearchBar = ({ onSearch, placeholder = "Search for TV shows...", className = "", variant = "dark" }: SearchBarProps) => {
  const [query, setQuery] = useState('');
  const [isInitialized, setIsInitialized] = useState(false);

  useEffect(() => {
    setIsInitialized(true);
  }, []);

  useEffect(() => {
    if (!isInitialized) return; // Don't search on initial load
    
    const debounceTimer = setTimeout(() => {
      onSearch(query);
    }, 300);

    return () => clearTimeout(debounceTimer);
  }, [query, onSearch, isInitialized]);

  const isDark = variant === 'dark';
  
  return (
    <div className={`relative w-full max-w-md mx-auto ${className}`}>
      <Search className={`absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 ${
        isDark ? 'text-slate-400' : 'text-muted-foreground'
      }`} />
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={placeholder}
        className={`w-full pl-10 h-10 text-base rounded-lg focus:outline-none focus:ring-2 transition-colors ${
          isDark 
            ? 'bg-slate-800 text-white placeholder-slate-400 border border-slate-600 focus:ring-blue-500 focus:border-blue-500'
            : 'bg-card text-foreground placeholder-muted-foreground border border-border focus:ring-primary focus:border-primary'
        }`}
      />
    </div>
  );
};