import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import { Eye, EyeOff, Zap } from "lucide-react";

interface SpoilerCardProps {
  episode: string;
  description: string;
}

export function SpoilerCard({ episode, description }: SpoilerCardProps) {
  const [isRevealed, setIsRevealed] = useState(false);

  return (
    <Card className="relative shadow-sm">
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2 text-red-600">
            <Zap className="w-5 h-5" />
            Peak Content
            <Badge variant="destructive" className="text-sm ml-2 px-2 py-1">
              SPOILERS
            </Badge>
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setIsRevealed(!isRevealed)}
            className="flex items-center gap-1 h-8 text-sm px-3"
          >
            {isRevealed ? (
              <>
                <EyeOff className="w-4 h-4" />
                Hide
              </>
            ) : (
              <>
                <Eye className="w-4 h-4" />
                Reveal
              </>
            )}
          </Button>
        </div>
      </CardHeader>
      <CardContent className="relative pt-0">
        <div className="mb-3">
          <Badge variant="outline" className={`mb-2 text-sm px-3 py-1 ${!isRevealed ? 'blur-sm' : ''}`}>
            {episode}
          </Badge>
        </div>
        <div className="relative">
          <p className={`text-sm leading-relaxed transition-all duration-300 ${
            !isRevealed ? 'blur-md select-none' : ''
          }`}>
            {description}
          </p>
          {!isRevealed && (
            <div className="absolute inset-0 bg-gradient-to-r from-transparent via-muted/20 to-transparent pointer-events-none" />
          )}
        </div>
      </CardContent>
    </Card>
  );
}