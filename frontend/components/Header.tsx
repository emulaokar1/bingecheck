'use client'

import Link from 'next/link';
import { SearchBar } from './SearchBar';

interface HeaderProps {
  showSearch?: boolean;
  onSearch?: (query: string) => void;
}

export function Header({ showSearch = true, onSearch }: HeaderProps) {
  return (
    <header className="border-b border-border/40 bg-card">
      <div className="container mx-auto px-4 py-6">
        <div className="text-center space-y-4">
          <Link href="/">
            <h1 className="text-4xl font-bold text-primary hover:text-primary/80 transition-colors cursor-pointer">
              BingeCheck
            </h1>
          </Link>
          <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
            Discover when TV shows get good, how they end, and what makes them worth your time
          </p>
          {showSearch && onSearch && (
            <SearchBar onSearch={onSearch} className="mt-6" />
          )}
        </div>
      </div>
    </header>
  );
}