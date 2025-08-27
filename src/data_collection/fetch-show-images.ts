import { createClient } from '@supabase/supabase-js'
import { readFileSync } from 'fs'
import { join } from 'path'

// Load environment variables from root .env file
try {
  const envContent = readFileSync(join(__dirname, '..', '..', '.env'), 'utf-8')
  envContent.split('\n').forEach(line => {
    const [key, ...valueParts] = line.split('=')
    if (key && valueParts.length > 0) {
      const value = valueParts.join('=').trim()
      process.env[key.trim()] = value
    }
  })
} catch (error) {
  console.error('Could not load .env file from root directory')
}

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY! 
)

const TMDB_API_KEY = process.env.TMDB_API_KEY!
const TMDB_BASE_URL = 'https://api.themoviedb.org/3'
const IMAGE_BASE_URL = 'https://image.tmdb.org/t/p'

interface TMDBImages {
  posters: Array<{ file_path: string; vote_average: number }>
  backdrops: Array<{ file_path: string; vote_average: number }>
  logos: Array<{ file_path: string }>
}

// Rate limiting helper
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

async function getTMDBShowId(imdbId: string): Promise<number | null> {
  try {
    const response = await fetch(
      `${TMDB_BASE_URL}/find/${imdbId}?api_key=${TMDB_API_KEY}&external_source=imdb_id`
    )
    const data = await response.json()
    return data.tv_results[0]?.id || null
  } catch (error) {
    console.error(`Error finding TMDB ID for ${imdbId}:`, error)
    return null
  }
}

async function getTMDBImages(tmdbId: number): Promise<TMDBImages | null> {
  try {
    // Request English images first, then fallback to all images
    const englishResponse = await fetch(
      `${TMDB_BASE_URL}/tv/${tmdbId}/images?api_key=${TMDB_API_KEY}&include_image_language=en,null`
    )
    const englishData = await englishResponse.json()
    
    // If we got good English images, use those
    if (englishData.posters?.length > 0 && englishData.backdrops?.length > 0) {
      return englishData
    }
    
    // Fallback to all languages if English selection is poor
    const response = await fetch(
      `${TMDB_BASE_URL}/tv/${tmdbId}/images?api_key=${TMDB_API_KEY}`
    )
    const data = await response.json()
    return data
  } catch (error) {
    console.error(`Error fetching images for TMDB ID ${tmdbId}:`, error)
    return null
  }
}

function selectBestImages(images: TMDBImages) {
  // Helper function to select best image with language preference
  const selectBestImage = (imageArray: Array<any>) => {
    if (!imageArray || imageArray.length === 0) return null

    // Priority 1: English images with good rating (7.0+)
    const englishHighQuality = imageArray.filter(img => 
      img.iso_639_1 === 'en' && img.vote_average >= 7.0
    ).sort((a, b) => b.vote_average - a.vote_average)[0]
    
    if (englishHighQuality) return englishHighQuality

    // Priority 2: Any English image
    const englishAny = imageArray.filter(img => 
      img.iso_639_1 === 'en'
    ).sort((a, b) => b.vote_average - a.vote_average)[0]
    
    if (englishAny) return englishAny

    // Priority 3: No language specified (often English or universal)
    const noLanguage = imageArray.filter(img => 
      !img.iso_639_1 || img.iso_639_1 === null
    ).sort((a, b) => b.vote_average - a.vote_average)[0]
    
    if (noLanguage) return noLanguage

    // Priority 4: Highest rated regardless of language
    return imageArray.sort((a, b) => b.vote_average - a.vote_average)[0]
  }

  const bestPoster = selectBestImage(images.posters)
  const bestBackdrop = selectBestImage(images.backdrops)

  return {
    poster: bestPoster ? `${IMAGE_BASE_URL}/w500${bestPoster.file_path}` : null,
    backdrop: bestBackdrop ? `${IMAGE_BASE_URL}/w1920${bestBackdrop.file_path}` : null
  }
}

async function updateShowImages() {
  // Get all shows that don't have images yet
  const { data: shows, error } = await supabase
    .from('shows')
    .select('id, imdb_id, title')
    // .is('poster_url', null)
    .limit(50) // Process in batches

  if (error) {
    console.error('Error fetching shows:', error)
    return
  }

  console.log(`Processing ${shows.length} shows...`)

  for (let i = 0; i < shows.length; i++) {
    const show = shows[i]
    console.log(`${i + 1}/${shows.length}: ${show.title}`)

    try {
      // Get TMDB ID
      const tmdbId = await getTMDBShowId(show.imdb_id)
      if (!tmdbId) {
        console.log(`  No TMDB match found`)
        continue
      }

      // Get images
      const images = await getTMDBImages(tmdbId)
      if (!images) {
        console.log(`  No images found`)
        continue
      }

      // Select best images
      const selectedImages = selectBestImages(images)

      // Update database
      const { error: updateError } = await supabase
        .from('shows')
        .update({
          poster_url: selectedImages.poster,
          backdrop_url: selectedImages.backdrop
        })
        .eq('id', show.id)

      if (updateError) {
        console.error(`  Error updating ${show.title}:`, updateError)
      } else {
        console.log(`  ✅ Updated with images`)
      }

      // Rate limiting - TMDB allows 40 requests per 10 seconds
      await sleep(300) // 300ms between requests = ~3 requests/second

    } catch (error) {
      console.error(`  Error processing ${show.title}:`, error)
    }
  }

  console.log('Image fetching complete!')
}

// Run the script
updateShowImages()