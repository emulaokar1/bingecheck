'use client'

import { useState, useEffect } from 'react';
import { Badge } from "./ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Button } from "./ui/button";
import { Star, MessageCircle, TrendingUp, Heart, Clock, ThumbsUp, AlertTriangle, Zap } from "lucide-react";
import { RatingTrendChart } from "./RatingTrendChart";
import { SpoilerCard } from "./SpoilerCard";
import { ImageWithFallback } from "./figma/ImageWithFallback";
import { NavigationHeader } from "./NavigationHeader";
import { RelatedShows } from "./RelatedShows";
import { Show, ShowAnalysis } from '../lib/supabase';
import { getShowData } from '../data/mockData';
import { getShowAnalysis } from '../lib/api';

interface ShowPageProps {
  showId: number;
}

export function ShowPage({ showId }: ShowPageProps) {
  const [show, setShow] = useState<Show | null>(null);
  const [analysis, setAnalysis] = useState<ShowAnalysis | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchShowData = async () => {
      setLoading(true);
      try {
        const [showData, analysisData] = await Promise.all([
          getShowData(showId),
          getShowAnalysis(showId)
        ]);
        
        setShow(showData);
        setAnalysis(analysisData);
      } catch (error) {
        console.error('Error fetching show data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchShowData();
  }, [showId]);

  if (loading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-primary"></div>
          <p className="mt-4 text-muted-foreground">Loading show details...</p>
        </div>
      </div>
    );
  }

  if (!show) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold mb-4">Show not found</h1>
          <p className="text-muted-foreground">The show you&apos;re looking for doesn&apos;t exist.</p>
        </div>
      </div>
    );
  }
  const handleSearch = (query: string) => {
    // Navigate to home page with search
    window.location.href = `/?search=${encodeURIComponent(query)}`;
  };

  return (
    <div className="min-h-screen bg-background">
      <NavigationHeader onSearch={handleSearch} showSearch={true} />
      
      <div className="mx-auto pl-4 pr-6 py-8 max-w-screen-2xl w-full">
        <div className="flex gap-12">
          {/* Left Sidebar - Related Shows */}
          <div className="hidden lg:block">
            <RelatedShows currentShow={{ id: show.id, genres: show.genres }} />
          </div>
          
          {/* Main Content */}
          <div className="flex-1">
      {/* Top Section */}
      <div className="flex flex-col lg:flex-row gap-6 mb-8">
        {/* Show Info */}
        <div className="flex-1">
          <div className="flex flex-wrap items-center gap-2 mb-2">
            <h1 className="text-4xl mr-4">{show.title}</h1>
            <span className="text-muted-foreground">({show.start_year}{show.end_year ? `-${show.end_year}` : ''})</span>
          </div>
          
          <div className="flex flex-wrap items-center gap-2 mb-2">
            {show.genres.map((genre) => (
              <Badge key={genre} variant="secondary" className="text-xs">
                {genre}
              </Badge>
            ))}
          </div>
          
          <div className="flex items-center gap-6 text-sm">
            <div className="flex items-center gap-1">
              <Star className="w-4 h-4 fill-yellow-400 text-yellow-400" />
              <span className="font-medium">{show.average_rating}</span>
              <span className="text-muted-foreground">IMDB</span>
            </div>
            <div className="flex items-center gap-1">
              <MessageCircle className="w-4 h-4 text-orange-500" />
              <span className="font-medium">{show.num_votes.toLocaleString()}</span>
              <span className="text-muted-foreground">votes</span>
            </div>
          </div>
        </div>
        
        {/* Right Side: Image and Chart Side by Side */}
        <div className="lg:w-[480px] flex gap-6">
          {/* Show Wallpaper */}
          <div className="flex-1 aspect-video rounded-lg overflow-hidden bg-background">
            <ImageWithFallback 
              src={show.backdrop_url || "https://images.unsplash.com/photo-1578662996442-48f60103fc96?w=400&h=225&fit=crop&crop=center"}
              alt={`${show.title} backdrop`}
              className="w-full h-full object-contain"
            />
          </div>
          
          {/* Rating Trend Chart */}
          <Card className="flex-1 shadow-sm">
            <CardHeader className="pb-1">
              <CardTitle className="text-sm font-semibold flex items-center gap-1 text-blue-600">
                <TrendingUp className="w-4 h-4" />
                Rating Trend
              </CardTitle>
            </CardHeader>
            <CardContent className="pt-0">
              <RatingTrendChart />
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
        {/* Why Viewers Like It */}
        <Card className="shadow-sm">
          <CardHeader className="pb-6">
            <CardTitle className="text-xl font-semibold flex items-center gap-2 text-pink-600">
              <Heart className="w-6 h-6" />
              Why Viewers Love It
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0 space-y-4">
            {analysis?.viewer_appeal ? (
              <p className="text-base leading-relaxed">{analysis.viewer_appeal}</p>
            ) : (
              <p className="text-base text-muted-foreground">Analysis not available yet.</p>
            )}
          </CardContent>
        </Card>

        {/* When It Gets Good */}
        <Card className="shadow-sm">
          <CardHeader className="pb-6">
            <CardTitle className="text-xl font-semibold flex items-center gap-2 text-purple-600">
              <Clock className="w-6 h-6" />
              When It Gets Good
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="mb-3">
              {analysis?.when_gets_good && (
                <p className="text-base leading-relaxed">{analysis.when_gets_good}</p>
              )}
              {!analysis?.when_gets_good && (
                <p className="text-base text-muted-foreground">Analysis not available yet.</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Ending Sentiment */}
        <Card className="shadow-sm">
          <CardHeader className="pb-6">
            <CardTitle className="text-xl font-semibold flex items-center gap-2 text-emerald-600">
              <Star className="w-6 h-6" />
              Ending Sentiment
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="mb-3">
              {analysis?.ending_sentiment && (
                <p className="text-base leading-relaxed">{analysis.ending_sentiment}</p>
              )}
              {!analysis?.ending_sentiment && (
                <p className="text-base text-muted-foreground">Analysis not available yet.</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Peak Content (Spoiler) */}
        <SpoilerCard 
          episode={analysis?.best_content_type || "Peak Content"}
          description={analysis?.best_content_description || "Analysis not available yet."}
        />
      </div>

          {/* Mobile Related Shows - Below main content */}
          <div className="lg:hidden mt-8">
            <RelatedShows currentShow={{ id: show.id, genres: show.genres }} />
          </div>
          </div>
          
        </div>
      </div>
    </div>
  );
}