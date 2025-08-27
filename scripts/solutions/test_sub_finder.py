# Test subreddit finding without full enrichment

from reddit_enricher import RedditEnricher

def test_subreddit_finding():
    enricher = RedditEnricher()
    
    test_shows = [
        "The Lord of the Rings: The Rings of Power",
        "The Queen's Gambit",
        "The Big Bang Theory",
        "Avatar: The Last Airbender",
        "Mr. Robot",
        "Modern Family"
    ]
    
    print("Testing subreddit finder:")
    print("=" * 60)
    
    for show in test_shows:
        subreddit = enricher.find_subreddit_with_gpt(show)
        print(f"{show:<40} → r/{subreddit if subreddit else 'NOT FOUND'}")

if __name__ == "__main__":
    test_subreddit_finding()