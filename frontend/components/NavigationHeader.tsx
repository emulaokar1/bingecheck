'use client'

import Link from 'next/link';
import { SimpleSearchBar } from './SimpleSearchBar';
import { Search } from 'lucide-react';
import { useState } from 'react';

interface NavigationHeaderProps {
  onSearch?: (query: string) => void;
  showSearch?: boolean;
}

export function NavigationHeader({ onSearch, showSearch = true }: NavigationHeaderProps) {
  const [isMobileSearchOpen, setIsMobileSearchOpen] = useState(false);

  const handleSearch = (query: string) => {
    if (onSearch) {
      onSearch(query);
    }
    // Close mobile search after search
    setIsMobileSearchOpen(false);
  };

  return (
    <header className="bg-slate-900 border-b border-slate-700 sticky top-0 z-50">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo/Brand - Far Left */}
          <div className="flex-shrink-0">
            <Link href="/" className="flex items-center">
              <h1 className="text-2xl font-bold text-white hover:text-blue-400 transition-colors">
                BingeCheck
              </h1>
            </Link>
          </div>

          {/* Search Bar - Center (Desktop) */}
          {showSearch && (
            <div className="hidden md:flex flex-1 max-w-2xl mx-8">
              <div className="w-full">
                <SimpleSearchBar 
                  onSearch={handleSearch} 
                  className="w-full max-w-none"
                />
              </div>
            </div>
          )}

          {/* Mobile Search Toggle */}
          {showSearch && (
            <div className="md:hidden">
              <button
                onClick={() => setIsMobileSearchOpen(!isMobileSearchOpen)}
                className="p-2 text-slate-300 hover:text-white hover:bg-slate-800 rounded-md transition-colors"
                aria-label="Toggle search"
              >
                <Search className="h-5 w-5" />
              </button>
            </div>
          )}

          {/* Right side spacer for balance */}
          <div className="hidden md:block flex-shrink-0 w-24"></div>
        </div>

        {/* Mobile Search Bar (Collapsible) */}
        {showSearch && isMobileSearchOpen && (
          <div className="md:hidden pb-4 pt-2">
            <SimpleSearchBar 
              onSearch={handleSearch} 
              className="w-full"
            />
          </div>
        )}
      </div>
    </header>
  );
}