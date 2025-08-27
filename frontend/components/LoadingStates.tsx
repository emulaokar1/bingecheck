import { Skeleton } from './ui/skeleton';
import { Card, CardContent } from './ui/card';

export const ShowCardSkeleton = () => {
  return (
    <Card className="h-full">
      <CardContent className="p-0">
        <Skeleton className="aspect-[2/3] w-full rounded-t-lg" />
        <div className="p-4 space-y-3">
          <div>
            <Skeleton className="h-6 w-3/4" />
            <Skeleton className="h-4 w-16 mt-1" />
          </div>
          <div className="flex items-center justify-between">
            <Skeleton className="h-4 w-12" />
            <Skeleton className="h-4 w-8" />
          </div>
          <div className="flex gap-1">
            <Skeleton className="h-5 w-16" />
            <Skeleton className="h-5 w-20" />
          </div>
          <Skeleton className="h-5 w-32" />
          <Skeleton className="h-5 w-24" />
        </div>
      </CardContent>
    </Card>
  );
};

export const ShowCardGrid = ({ count = 8 }: { count?: number }) => {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
      {Array.from({ length: count }).map((_, i) => (
        <ShowCardSkeleton key={i} />
      ))}
    </div>
  );
};

export const ShowDetailSkeleton = () => {
  return (
    <div className="min-h-screen bg-background">
      {/* Hero Section */}
      <div className="relative bg-gradient-to-b from-card to-background">
        <div className="container mx-auto px-4 py-12">
          <div className="flex flex-col lg:flex-row gap-8">
            <Skeleton className="w-full max-w-sm aspect-[2/3] mx-auto lg:mx-0" />
            <div className="flex-1 space-y-6">
              <div>
                <Skeleton className="h-12 w-3/4 mb-2" />
                <Skeleton className="h-6 w-32" />
              </div>
              <div className="flex flex-wrap gap-2">
                <Skeleton className="h-6 w-16" />
                <Skeleton className="h-6 w-20" />
                <Skeleton className="h-6 w-18" />
              </div>
              <div className="flex items-center gap-4">
                <Skeleton className="h-8 w-20" />
                <Skeleton className="h-8 w-24" />
              </div>
              <Skeleton className="h-20 w-full" />
            </div>
          </div>
        </div>
      </div>
      
      {/* Content Sections */}
      <div className="container mx-auto px-4 py-8 space-y-12">
        <div>
          <Skeleton className="h-8 w-48 mb-4" />
          <Skeleton className="h-24 w-full" />
        </div>
        
        <div>
          <Skeleton className="h-8 w-40 mb-4" />
          <Skeleton className="h-32 w-full" />
        </div>
        
        <div>
          <Skeleton className="h-8 w-36 mb-4" />
          <Skeleton className="h-64 w-full" />
        </div>
      </div>
    </div>
  );
};