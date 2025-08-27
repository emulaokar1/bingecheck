'use client'

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { Star, MessageSquare } from 'lucide-react';
import { Card, CardContent } from './ui/card';
import { Badge } from './ui/badge';
import { Show, EndingSentiment } from '../types';
import { getRelatedShows } from '../lib/api';

interface RelatedShowsProps {
  currentShow: {
    id: number;
    genres: string[];
  };
}

const getSentimentColor = (sentiment: EndingSentiment) => {
  switch (sentiment) {
    case 'praised':
      return 'bg-green-500/10 text-green-600 border-green-500/20';
    case 'mixed':
      return 'bg-yellow-500/10 text-yellow-600 border-yellow-500/20';
    case 'disappointing':
      return 'bg-red-500/10 text-red-600 border-red-500/20';
    default:
      return 'bg-muted text-muted-foreground border-border';
  }
};

const getSentimentText = (sentiment: EndingSentiment) => {
  switch (sentiment) {
    case 'praised':
      return 'Acclaimed ending';
    case 'mixed':
      return 'Mixed ending';
    case 'disappointing':
      return 'Disappointing ending';
    default:
      return 'No ending data';
  }
};

export const RelatedShows = ({ currentShow, className = "" }: RelatedShowsProps & { className?: string }) => {
  const [relatedShows, setRelatedShows] = useState<Show[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchRelatedShows = async () => {
      setLoading(true);
      try {
        const shows = await getRelatedShows(currentShow.genres, currentShow.id, 6);
        setRelatedShows(shows);
      } catch (error) {
        console.error('Error fetching related shows:', error);
        setRelatedShows([]);
      } finally {
        setLoading(false);
      }
    };

    fetchRelatedShows();
  }, [currentShow.id, currentShow.genres]);

  if (loading) {
    return (
      <div className={`space-y-3 ${className}`}>
        <h2 className="text-lg font-semibold">Related Shows</h2>
        <div className="hidden lg:block space-y-3 w-64 ml-2">
          {[1, 2, 3].map((i) => (
            <div key={i} className="animate-pulse">
              <div className="h-24 bg-muted rounded"></div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (relatedShows.length === 0) {
    return null;
  }

  return (
    <div className={`space-y-3 ${className}`}>
      <h2 className="text-lg font-semibold">Related Shows</h2>
      
      {/* Desktop: Vertical stack */}
      <div className="hidden lg:block space-y-3 w-64 ml-2">
        {relatedShows.map((show) => (
          <Link key={show.id} href={`/shows/${show.id}`}>
            <Card className="group cursor-pointer transition-all duration-300 hover:shadow-md hover:scale-[1.02] border-border/50">
              <CardContent className="p-0">
                <div className="flex gap-3 p-3">
                  {/* Compact Poster */}
                  <div className="w-16 h-24 flex-shrink-0 overflow-hidden rounded">
                    <img
                      src={show.poster_url || `https://images.unsplash.com/photo-1461749280684-dccba630e2f6?w=300&h=450&fit=crop&auto=format`}
                      alt={show.title}
                      className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
                    />
                  </div>
                  
                  {/* Content */}
                  <div className="flex-1 space-y-1">
                    {/* Title and Year */}
                    <div>
                      <h3 className="font-medium text-sm text-foreground group-hover:text-primary transition-colors line-clamp-1">
                        {show.title}
                      </h3>
                      <p className="text-xs text-muted-foreground">{show.start_year}</p>
                    </div>
                    
                    {/* Rating and Discussions */}
                    <div className="flex items-center justify-between text-xs">
                      <div className="flex items-center gap-1">
                        <Star className="h-3 w-3 fill-yellow-400 text-yellow-400" />
                        <span className="font-medium">{show.average_rating}</span>
                      </div>
                      <div className="flex items-center gap-1 text-muted-foreground">
                        <MessageSquare className="h-3 w-3" />
                        <span>{show.num_votes}</span>
                      </div>
                    </div>
                    
                    {/* Primary Genre */}
                    <div>
                      <Badge variant="secondary" className="text-xs py-0 px-1.5 h-4">
                        {show.genres[0]}
                      </Badge>
                    </div>
                    
                    {/* Ending Sentiment */}
                    {show.ending_sentiment && (
                      <Badge className={`text-xs py-0 px-1.5 h-4 ${getSentimentColor(show.ending_sentiment as EndingSentiment)}`}>
                        {getSentimentText(show.ending_sentiment as EndingSentiment)}
                      </Badge>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
      
      {/* Mobile: Horizontal scroll */}
      <div className="lg:hidden">
        <div className="flex gap-3 overflow-x-auto pb-3 scrollbar-hide">
          {relatedShows.map((show) => (
            <Link key={show.id} href={`/shows/${show.id}`} className="flex-shrink-0">
              <Card className="group cursor-pointer transition-all duration-300 hover:shadow-md border-border/50 w-48">
                <CardContent className="p-0">
                  <div className="flex gap-3 p-3">
                    {/* Compact Poster */}
                    <div className="w-12 h-18 flex-shrink-0 overflow-hidden rounded">
                      <img
                        src={show.poster_url || `https://images.unsplash.com/photo-1461749280684-dccba630e2f6?w=300&h=450&fit=crop&auto=format`}
                        alt={show.title}
                        className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
                      />
                    </div>
                    
                    {/* Content */}
                    <div className="flex-1 space-y-1">
                      {/* Title and Year */}
                      <div>
                        <h3 className="font-medium text-sm text-foreground group-hover:text-primary transition-colors line-clamp-1">
                          {show.title}
                        </h3>
                        <p className="text-xs text-muted-foreground">{show.start_year}</p>
                      </div>
                      
                      {/* Rating */}
                      <div className="flex items-center gap-1 text-xs">
                        <Star className="h-3 w-3 fill-yellow-400 text-yellow-400" />
                        <span className="font-medium">{show.average_rating}</span>
                      </div>
                      
                      {/* Primary Genre */}
                      <Badge variant="secondary" className="text-xs py-0 px-1.5 h-4">
                        {show.genres[0]}
                      </Badge>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      </div>
    </div>
  );
};