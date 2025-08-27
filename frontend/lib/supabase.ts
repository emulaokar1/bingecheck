import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

if (!supabaseUrl || !supabaseAnonKey) {
  console.error('Missing Supabase environment variables:', {
    url: !!supabaseUrl,
    key: !!supabaseAnonKey
  })
}

console.log('Supabase configuration:', {
  url: supabaseUrl ? 'Set' : 'Missing',
  key: supabaseAnonKey ? 'Set' : 'Missing'
})

export const supabase = createClient(supabaseUrl, supabaseAnonKey)

export type Show = {
  id: number
  imdb_id: string
  title: string
  start_year: number
  end_year?: number
  runtime_minutes?: number
  genres: string[]
  average_rating: number
  num_votes: number
  when_gets_good?: string
  ending_sentiment?: string
  best_content?: {
    type: string
    description: string
  }
  viewer_appeal?: string
  total_discussions?: number
  poster_url?: string
  backdrop_url?: string
}

export type ShowAnalysis = {
  id: number
  imdb_id: string
  title: string
  start_year: number
  end_year?: number
  genres: string[]
  average_rating: number
  num_votes: number
  total_discussions: number
  when_gets_good: string
  ending_sentiment: string
  best_content_type: string
  best_content_description: string
  viewer_appeal: string
  last_calculated: string
}