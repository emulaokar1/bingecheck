import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip } from "recharts";

const trendData = [
  { season: "S1", rating: 8.2, episode: "Season 1" },
  { season: "S2", rating: 8.7, episode: "Season 2" },
  { season: "S3", rating: 9.1, episode: "Season 3" },
  { season: "S4", rating: 9.6, episode: "Season 4" },
  { season: "S5", rating: 9.8, episode: "Season 5" }
];

export function RatingTrendChart() {
  return (
    <div className="h-24">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={trendData} margin={{ top: 2, right: 2, left: 2, bottom: 2 }}>
          <XAxis 
            dataKey="season" 
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
          />
          <YAxis 
            domain={[7.5, 10]}
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
            width={25}
          />
          <Tooltip 
            contentStyle={{
              backgroundColor: 'hsl(var(--popover))',
              border: '1px solid hsl(var(--border))',
              borderRadius: '6px',
              fontSize: '11px'
            }}
            labelFormatter={(value) => `${value}`}
            formatter={(value) => [`${value}`, 'Rating']}
          />
          <Line 
            type="monotone" 
            dataKey="rating" 
            stroke="hsl(var(--primary))" 
            strokeWidth={2}
            dot={{ fill: 'hsl(var(--primary))', strokeWidth: 2, r: 2 }}
            activeDot={{ r: 3, stroke: 'hsl(var(--primary))', strokeWidth: 2 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}