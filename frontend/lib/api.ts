import { supabase, Show, ShowAnalysis } from './supabase'

export async function getShowById(id: number): Promise<Show | null> {
  try {
    const { data, error } = await supabase
      .from('shows')
      .select('*')
      .eq('id', id)
      .single()

    if (error) {
      console.error('Error fetching show:', error)
      console.error('Attempted to fetch show with id:', id)
      return null
    }

    return data
  } catch (error) {
    console.error('Error fetching show:', error)
    return null
  }
}

export async function getShowAnalysis(showId: number): Promise<ShowAnalysis | null> {
  try {
    // LLM analysis data is in the show_summary table
    const { data, error } = await supabase
      .from('show_summary')
      .select('*')
      .eq('id', showId)
      .single()

    if (error) {
      console.error('Error fetching show analysis:', error)
      console.error('Attempted to fetch analysis for showId:', showId)
      return null
    }

    return data
  } catch (error) {
    console.error('Error fetching show analysis:', error)
    return null
  }
}

export async function getRelatedShows(genres: string[], excludeId: number, limit: number = 6): Promise<Show[]> {
  try {
    const { data, error } = await supabase
      .from('shows')
      .select('*')
      .neq('id', excludeId)
      .overlaps('genres', genres)
      .order('average_rating', { ascending: false })
      .limit(limit)

    if (error) {
      console.error('Error fetching related shows:', error)
      return []
    }

    return data || []
  } catch (error) {
    console.error('Error fetching related shows:', error)
    return []
  }
}

export async function getAllShows(): Promise<Show[]> {
  try {
    console.log('Fetching shows from Supabase...')
    const { data, error } = await supabase
      .from('shows')
      .select('*')
      .order('average_rating', { ascending: false })

    if (error) {
      console.error('Supabase error fetching shows:', error)
      throw new Error(`Database error: ${error.message}`)
    }

    console.log('Successfully fetched shows:', data?.length || 0)
    return data || []
  } catch (error) {
    console.error('Error fetching shows:', error)
    throw error // Re-throw to handle in component
  }
}

export async function searchShows(query: string): Promise<Show[]> {
  try {
    console.log('Searching shows for query:', query)
    const { data, error } = await supabase
      .from('shows')
      .select('*')
      .ilike('title', `%${query}%`)
      .order('average_rating', { ascending: false })

    if (error) {
      console.error('Supabase error searching shows:', error)
      throw new Error(`Database error: ${error.message}`)
    }

    console.log('Search results:', data?.length || 0)
    return data || []
  } catch (error) {
    console.error('Error searching shows:', error)
    throw error // Re-throw to handle in component
  }
}