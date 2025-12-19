"""
Insert Steam Tags Data

Loads scraped tag data from JSONL file into PostgreSQL database.
Follows the same patterns as insert_app_details.py.

Usage:
    python insert_tags.py                           # Default: data/steamspy_raw.jsonl
    python insert_tags.py --input data/tags.jsonl   # Custom input file
    python insert_tags.py --dry-run                 # Preview without inserting
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

DEFAULT_INPUT = Path("data/steam_tags.jsonl")


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
    
    Returns dict with:
        - tags: list of (tag_id, name) tuples
        - game_tags: list of (appid, tag_id, votes, scraped_at) tuples
        - stats: summary statistics
    """
    tags = {}  # tagid -> name (deduped)
    game_tags = []  # (appid, tagid, votes, scraped_at)
    
    apps_processed = 0
    apps_skipped_no_tags = 0
    duplicate_tag_names = Counter()  # Track if same name has different IDs
    
    for record in records:
        appid = record['appid']
        scraped_at = record.get('scraped_at')  # ISO format timestamp
        tag_list = record.get('data', {}).get('tags', [])
        
        if not tag_list:
            apps_skipped_no_tags += 1
            continue
        
        apps_processed += 1
        
        for tag in tag_list:
            tagid = tag['tagid']
            name = tag['name']
            votes = tag.get('count', 0)
            
            # Check for duplicate names with different IDs
            if name in [t[1] for t in tags.items() if t[0] != tagid]:
                existing_id = [t[0] for t in tags.items() if t[1] == name][0]
                duplicate_tag_names[name] += 1
                # Keep the first ID we saw for this name
                tagid = existing_id
            
            tags[tagid] = name
            game_tags.append((appid, tagid, votes, scraped_at))
    
    # Dedupe game_tags - now using (appid, tagid) as the key, keeping first occurrence
    seen = set()
    game_tags_deduped = []
    for gt in game_tags:
        key = (gt[0], gt[1])  # (appid, tagid)
        if key not in seen:
            seen.add(key)
            game_tags_deduped.append(gt)
    
    return {
        'tags': [(tid, name) for tid, name in tags.items()],
        'game_tags': game_tags_deduped,
        'stats': {
            'apps_processed': apps_processed,
            'apps_skipped_no_tags': apps_skipped_no_tags,
            'unique_tags': len(tags),
            'total_game_tag_rows': len(game_tags_deduped),
            'duplicate_tag_names': dict(duplicate_tag_names) if duplicate_tag_names else None
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
    Bulk insert all tag data.
    
    Args:
        prepared: Output from prepare_data()
        dry_run: If True, only print what would be done
    """
    stats = prepared['stats']
    
    print(f"\n{'='*50}")
    print("DATA SUMMARY")
    print(f"{'='*50}")
    print(f"Apps with tags:      {stats['apps_processed']}")
    print(f"Apps without tags:   {stats['apps_skipped_no_tags']}")
    print(f"Unique tags:         {stats['unique_tags']}")
    print(f"Game-tag rows:       {stats['total_game_tag_rows']}")
    
    if stats['duplicate_tag_names']:
        print(f"\nWARNING: Duplicate tag names with different IDs:")
        for name, count in stats['duplicate_tag_names'].items():
            print(f"  '{name}': {count} duplicates")
    
    if dry_run:
        print("\n[DRY RUN] No data inserted")
        print("\nSample tags:")
        for tid, name in prepared['tags'][:10]:
            print(f"  {tid}: {name}")
        return
    
    with get_db_connection() as conn:
        # Check which appids exist in games table
        game_tag_appids = {gt[0] for gt in prepared['game_tags']}
        valid_appids = check_foreign_keys(conn, game_tag_appids)
        
        missing_appids = game_tag_appids - valid_appids
        if missing_appids:
            print(f"\nWARNING: {len(missing_appids)} appids not in games table (will skip)")
        
        # Filter game_tags to only valid appids
        game_tags_filtered = [
            gt for gt in prepared['game_tags'] 
            if gt[0] in valid_appids
        ]
        
        with conn.cursor() as cur:
            print("\n  Inserting tags...")
            cur.executemany("""
                INSERT INTO tags (tag_id, name)
                VALUES (%s, %s)
                ON CONFLICT (tag_id) DO UPDATE SET
                    name = EXCLUDED.name
            """, prepared['tags'])
            print(f"    -> {len(prepared['tags'])} tags upserted")
            
            print("  Inserting game_tags...")
            cur.executemany("""
                INSERT INTO game_tags (appid, tag_id, votes, scraped_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (appid, tag_id) DO UPDATE SET
                    votes = EXCLUDED.votes,
                    scraped_at = EXCLUDED.scraped_at
            """, game_tags_filtered)
            print(f"    -> {len(game_tags_filtered)} game-tag relationships upserted")
            
            if missing_appids:
                print(f"    -> {len(missing_appids)} skipped (appid not in games)")
        
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
            # Count tags
            cur.execute("SELECT COUNT(*) FROM tags")
            tag_count = cur.fetchone()[0]
            print(f"Total tags in DB:        {tag_count}")
            
            # Count game_tags
            cur.execute("SELECT COUNT(*) FROM game_tags")
            game_tag_count = cur.fetchone()[0]
            print(f"Total game_tags in DB:   {game_tag_count}")
            
            # Games with tags
            cur.execute("SELECT COUNT(DISTINCT appid) FROM game_tags")
            games_with_tags = cur.fetchone()[0]
            print(f"Games with tags:         {games_with_tags}")
            
            # Top 10 tags
            cur.execute("""
                SELECT t.name, COUNT(*) as game_count, SUM(gt.votes) as total_votes
                FROM game_tags gt
                JOIN tags t ON gt.tag_id = t.tag_id
                GROUP BY t.tag_id, t.name
                ORDER BY game_count DESC
                LIMIT 10
            """)
            
            print("\nTop 10 tags by game count:")
            for name, count, votes in cur.fetchall():
                print(f"  {name}: {count} games, {votes:,} total votes")
            
            # Sample game with tags
            cur.execute("""
                SELECT g.appid, g.name, 
                       ARRAY_AGG(t.name ORDER BY gt.votes DESC) as tags
                FROM games g
                JOIN game_tags gt ON g.appid = gt.appid
                JOIN tags t ON gt.tag_id = t.tag_id
                GROUP BY g.appid, g.name
                LIMIT 3
            """)
            
            print("\nSample games with their tags:")
            for appid, name, tags in cur.fetchall():
                print(f"  {name} ({appid}):")
                print(f"    {', '.join(tags[:5])}...")


# ============ Main ============

def parse_args():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Insert Steam tag data into PostgreSQL"
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