'use client'

import Link from 'next/link';
import { Star, MessageSquare } from 'lucide-react';
import { Card, CardContent } from './ui/card';
import { Badge } from './ui/badge';
import { Show, EndingSentiment } from '../types';

interface ShowCardProps {
  show: Show;
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

export const ShowCard = ({ show }: ShowCardProps) => {
  return (
    <Link href={`/shows/${show.id}`}>
      <Card className="group cursor-pointer h-full transition-all duration-300 hover:shadow-glow hover:scale-[1.02] card-gradient border-border/50">
        <CardContent className="p-0">
          {/* Poster */}
          <div className="aspect-[2/3] overflow-hidden rounded-t-lg">
            <img
              src={show.poster_url || `https://images.unsplash.com/photo-1461749280684-dccba630e2f6?w=300&h=450&fit=crop&auto=format`}
              alt={show.title}
              className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
            />
          </div>
          
          {/* Content */}
          <div className="p-6 space-y-4">
            {/* Title and Year */}
            <div>
              <h3 className="font-semibold text-xl text-foreground group-hover:text-primary transition-colors line-clamp-1">
                {show.title}
              </h3>
              <p className="text-base text-muted-foreground">{show.start_year}</p>
            </div>
            
            {/* Rating and Genres */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-1">
                <Star className="h-4 w-4 fill-yellow-400 text-yellow-400" />
                <span className="text-base font-medium">{show.average_rating}</span>
              </div>
              <div className="flex items-center gap-1 text-muted-foreground">
                <MessageSquare className="h-4 w-4" />
                <span className="text-base">{show.num_votes}</span>
              </div>
            </div>
            
            {/* Genres */}
            <div className="flex flex-wrap gap-1">
              {show.genres.slice(0, 2).map((genre) => (
                <Badge key={genre} variant="secondary" className="text-sm px-3 py-1">
                  {genre}
                </Badge>
              ))}
              {show.genres.length > 2 && (
                <Badge variant="secondary" className="text-sm px-3 py-1">
                  +{show.genres.length - 2}
                </Badge>
              )}
            </div>
            
            {/* Gets Good Badge */}
            {show.when_gets_good && (
              <Badge className="bg-primary/10 text-primary border-primary/20 text-sm px-3 py-1">
                Gets good at {show.when_gets_good.split(' - ')[0]}
              </Badge>
            )}
            
            {/* Ending Sentiment */}
            {show.ending_sentiment && (
              <Badge className={`text-sm px-3 py-1 ${getSentimentColor(show.ending_sentiment as EndingSentiment)}`}>
                {getSentimentText(show.ending_sentiment as EndingSentiment)}
              </Badge>
            )}
          </div>
        </CardContent>
      </Card>
    </Link>
  );
};