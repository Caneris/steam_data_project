"""
Insert Steam Reviews Data

Loads scraped review data from JSONL file into PostgreSQL database.
Follows the same patterns as insert_tags.py.

Usage:
    python insert_reviews.py                           # Default: data/steam_reviews.jsonl
    python insert_reviews.py --input data/reviews.jsonl # Custom input file
    python insert_reviews.py --dry-run                 # Preview without inserting
"""

import json
import os
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime
from collections import Counter

import psycopg
from dotenv import load_dotenv

load_dotenv()

# ============ Configuration ============

DEFAULT_INPUT = Path("data/steam_reviews.jsonl")


# ============ Database Connection ============

@contextmanager
def get_db_connection():
    conn = psycopg.connect(
        host=os.getenv("DB_HOST"),
        dbname="steam_games",
        user="caneris",
        password=os.getenv("DB_PASSWORD")
    )
    try:
        yield conn
    finally:
        conn.close()


# ============ Data Preparation ============

def load_jsonl(path: Path) -> list[dict]:
    """Load all records from a JSONL file."""
    records = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def prepare_data(records: list[dict]) -> dict:
    """
    Transform raw scraped data into table-ready structures.
    
    Expected JSONL format:
    {
        "appid": 730,
        "scraped_at": "2025-01-15T12:00:00",
        "data": {
            "reviews_by_language": [
                {"language": "english", "language_name": "English", "count": 9425, "score": "Very Positive", "score_numeric": 8},
                ...
            ]
        }
    }
    
    Returns dict with:
        - game_reviews: list of (appid, language, review_count, score, score_numeric, snapshot_date) tuples
        - stats: summary statistics
    """
    game_reviews = []  # (appid, language, review_count, score, score_numeric, snapshot_date)
    
    apps_processed = 0
    apps_skipped_no_reviews = 0
    languages_seen = Counter()
    scores_seen = Counter()
    
    for record in records:
        appid = record['appid']
        scraped_at = record.get('scraped_at', '')
        
        # Extract date portion from ISO timestamp for snapshot_date
        snapshot_date = scraped_at[:10] if scraped_at else None
        
        reviews_by_lang = record.get('data', {}).get('reviews_by_language')
        
        if not reviews_by_lang:
            apps_skipped_no_reviews += 1
            continue
        
        apps_processed += 1
        
        for review in reviews_by_lang:
            language = review.get('language')
            review_count = review.get('count', 0)
            score = review.get('score')
            score_numeric = review.get('score_numeric')
            
            # Skip languages with no reviews
            if review_count == 0:
                continue
            
            languages_seen[language] += 1
            scores_seen[score] += 1
            game_reviews.append((appid, language, review_count, score, score_numeric, snapshot_date))
    
    # Dedupe by (appid, language, snapshot_date) - keep first occurrence
    seen = set()
    game_reviews_deduped = []
    for gr in game_reviews:
        key = (gr[0], gr[1], gr[5])  # (appid, language, snapshot_date)
        if key not in seen:
            seen.add(key)
            game_reviews_deduped.append(gr)
    
    return {
        'game_reviews': game_reviews_deduped,
        'stats': {
            'apps_processed': apps_processed,
            'apps_skipped_no_reviews': apps_skipped_no_reviews,
            'total_review_rows': len(game_reviews_deduped),
            'unique_languages': len(languages_seen),
            'top_languages': languages_seen.most_common(15),
            'score_distribution': scores_seen.most_common()
        }
    }


# ============ Database Operations ============

def check_foreign_keys(conn, appids: set[int]) -> set[int]:
    """
    Check which appids exist in the games table.
    Returns set of valid appids.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT appid FROM games")
        existing = {row[0] for row in cur.fetchall()}
    
    return appids & existing


def insert_batch(prepared: dict, dry_run: bool = False):
    """
    Bulk insert all review data.
    
    Args:
        prepared: Output from prepare_data()
        dry_run: If True, only print what would be done
    """
    stats = prepared['stats']
    
    print(f"\n{'='*50}")
    print("DATA SUMMARY")
    print(f"{'='*50}")
    print(f"Apps with reviews:      {stats['apps_processed']}")
    print(f"Apps without reviews:   {stats['apps_skipped_no_reviews']}")
    print(f"Unique languages:       {stats['unique_languages']}")
    print(f"Total review rows:      {stats['total_review_rows']}")
    
    print(f"\nTop languages by game count:")
    for lang, count in stats['top_languages']:
        print(f"  {lang}: {count} games")
    
    print(f"\nScore distribution:")
    for score, count in stats['score_distribution']:
        print(f"  {score}: {count}")
    
    if dry_run:
        print("\n[DRY RUN] No data inserted")
        print("\nSample rows:")
        for appid, lang, count, score, score_num, date in prepared['game_reviews'][:10]:
            print(f"  {appid} | {lang}: {count} reviews, {score} ({score_num}) [{date}]")
        return
    
    with get_db_connection() as conn:
        # Check which appids exist in games table
        review_appids = {gr[0] for gr in prepared['game_reviews']}
        valid_appids = check_foreign_keys(conn, review_appids)
        
        missing_appids = review_appids - valid_appids
        if missing_appids:
            print(f"\nWARNING: {len(missing_appids)} appids not in games table (will skip)")
        
        # Filter game_reviews to only valid appids
        game_reviews_filtered = [
            gr for gr in prepared['game_reviews'] 
            if gr[0] in valid_appids
        ]
        
        with conn.cursor() as cur:
            print("\n  Inserting game_reviews...")
            cur.executemany("""
                INSERT INTO game_reviews (appid, language, review_count, score, score_numeric, snapshot_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (appid, language, snapshot_date) DO UPDATE SET
                    review_count = EXCLUDED.review_count,
                    score = EXCLUDED.score,
                    score_numeric = EXCLUDED.score_numeric
            """, game_reviews_filtered)
            print(f"    -> {len(game_reviews_filtered)} review rows upserted")
            
            if missing_appids:
                print(f"    -> {len(missing_appids)} appids skipped (not in games)")
        
        conn.commit()
        print("\n  Committed successfully!")


# ============ Verification ============

def verify_insertion():
    """Run some basic queries to verify the data looks correct."""
    print(f"\n{'='*50}")
    print("VERIFICATION QUERIES")
    print(f"{'='*50}")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Count total rows
            cur.execute("SELECT COUNT(*) FROM game_reviews")
            total_rows = cur.fetchone()[0]
            print(f"Total review rows in DB:     {total_rows}")
            
            # Games with reviews
            cur.execute("SELECT COUNT(DISTINCT appid) FROM game_reviews")
            games_with_reviews = cur.fetchone()[0]
            print(f"Games with reviews:          {games_with_reviews}")
            
            # Unique languages
            cur.execute("SELECT COUNT(DISTINCT language) FROM game_reviews")
            unique_langs = cur.fetchone()[0]
            print(f"Unique languages:            {unique_langs}")
            
            # Top languages by total reviews
            cur.execute("""
                SELECT language, 
                       COUNT(DISTINCT appid) as game_count,
                       SUM(review_count) as total_reviews
                FROM game_reviews
                GROUP BY language
                ORDER BY total_reviews DESC
                LIMIT 10
            """)
            
            print("\nTop 10 languages by total reviews:")
            for lang, games, total in cur.fetchall():
                print(f"  {lang}: {games} games, {total:,} reviews")
            
            # Score distribution
            cur.execute("""
                SELECT score, COUNT(*) as count
                FROM game_reviews
                GROUP BY score
                ORDER BY count DESC
            """)
            
            print("\nScore distribution:")
            for score, count in cur.fetchall():
                print(f"  {score}: {count:,}")
            
            # Sample games with reviews
            cur.execute("""
                SELECT g.appid, g.name,
                       SUM(gr.review_count) as total_reviews,
                       COUNT(DISTINCT gr.language) as lang_count
                FROM games g
                JOIN game_reviews gr ON g.appid = gr.appid
                GROUP BY g.appid, g.name
                ORDER BY total_reviews DESC
                LIMIT 5
            """)
            
            print("\nTop 5 games by review count:")
            for appid, name, total, langs in cur.fetchall():
                print(f"  {name}: {total:,} reviews in {langs} languages")


# ============ Main ============

def parse_args():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Insert Steam review data into PostgreSQL"
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input JSONL file (default: {DEFAULT_INPUT})"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Preview data without inserting"
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip verification queries after insertion"
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    if not args.input.exists():
        print(f"ERROR: Input file not found: {args.input}")
        return 1
    
    print(f"Loading data from {args.input}...")
    records = load_jsonl(args.input)
    print(f"Loaded {len(records)} records")
    
    print("Preparing data...")
    prepared = prepare_data(records)
    
    insert_batch(prepared, dry_run=args.dry_run)
    
    if not args.dry_run and not args.skip_verify:
        verify_insertion()
    
    return 0


if __name__ == "__main__":
    exit(main())