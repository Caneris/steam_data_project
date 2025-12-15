from steam_api_scraper import CheckpointManager
from pathlib import Path
import json
import psycopg
from psycopg import sql
from datetime import datetime
from contextlib import contextmanager
import os
from dotenv import load_dotenv
from io import StringIO

# path to test data
FILENAME = "apps_data"
PATH = "checkpoints"

load_dotenv()

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


def parse_release_date(date_str: str) -> str | None:
    if not date_str:
        return None
    
    formats = ["%d %b, %Y", "%b %d, %Y", "%b %Y", "%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_required_age(value) -> int:
    """Convert required_age to integer, handling edge cases."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        # Handle full-width numbers (Japanese etc.)
        normalized = value.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        try:
            return int(normalized)
        except ValueError:
            return 0
    return 0


def prepare_data(data: dict) -> dict:
    """Transform raw scraped data into table-ready lists."""
    
    games = []
    genres = {}          # genre_id -> name (deduped)
    categories = {}      # category_id -> name
    category_name_to_id = {}  # name -> first genre_id (for deduping)
    developers = set()   # unique names
    publishers = set()
    
    game_genres = []
    game_categories = []
    game_developers = []
    game_publishers = []
    
    for appid, raw in data.items():
        if not raw or raw.get('type') != 'game':
            continue
        
        appid = int(appid)
        release_info = raw.get('release_date', {})
        release_date = None
        if not release_info.get('coming_soon'):
            release_date = parse_release_date(release_info.get('date', ''))
        
        games.append((
            appid,
            raw.get('name'),
            raw.get('is_free', False),
            release_date,
            release_info.get('coming_soon', False),
            raw.get('platforms', {}).get('windows', False),
            raw.get('platforms', {}).get('mac', False),
            raw.get('platforms', {}).get('linux', False),
            raw.get('metacritic', {}).get('score'),
            raw.get('recommendations', {}).get('total'),
            parse_required_age(raw.get('required_age'))
        ))
        
        for genre in raw.get('genres', []):
            gid = int(genre['id'])
            genres[gid] = genre['description']
            game_genres.append((appid, gid))
        
        for cat in raw.get('categories', []):
            cid = int(cat['id'])
            name = cat['description']
            
            # Dedupe: keep first ID for each name
            if name not in category_name_to_id:
                category_name_to_id[name] = cid
                categories[cid] = name
            
            # Always use the canonical ID for junction table
            canonical_id = category_name_to_id[name]
            game_categories.append((appid, canonical_id))
        
        for dev in raw.get('developers', []):
            developers.add(dev)
            game_developers.append((appid, dev))
        
        for pub in raw.get('publishers', []):
            publishers.add(pub)
            game_publishers.append((appid, pub))
    
    # Dedupe game_categories (same game might have both ID 30 and 51)
    game_categories = list(set(game_categories))
    
    return {
        'games': games,
        'genres': [(gid, name) for gid, name in genres.items()],
        'categories': [(cid, name) for cid, name in categories.items()],
        'developers': list(developers),
        'publishers': list(publishers),
        'game_genres': game_genres,
        'game_categories': game_categories,
        'game_developers': game_developers,
        'game_publishers': game_publishers,
    }


# def check_duplicates(data: dict):
#     """Check for duplicate names with different IDs."""
#     categories = {}
#     for appid, raw in data.items():
#         for cat in raw.get('categories', []):
#             name = cat['description']
#             cid = int(cat['id'])
#             if name in categories and categories[name] != cid:
#                 print(f"Category '{name}': ID {categories[name]} vs {cid}")
#             categories[name] = cid


def insert_batch(data: dict):
    """Bulk insert all data using efficient batch operations."""
    
    prepared = prepare_data(data)
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            
            print("  Inserting games...")
            cur.executemany("""
                INSERT INTO games (
                    appid, name, is_free, release_date, coming_soon,
                    platforms_windows, platforms_mac, platforms_linux,
                    metacritic_score, recommendations_total, required_age
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (appid) DO UPDATE SET
                    name = EXCLUDED.name,
                    is_free = EXCLUDED.is_free,
                    release_date = EXCLUDED.release_date,
                    coming_soon = EXCLUDED.coming_soon,
                    platforms_windows = EXCLUDED.platforms_windows,
                    platforms_mac = EXCLUDED.platforms_mac,
                    platforms_linux = EXCLUDED.platforms_linux,
                    metacritic_score = EXCLUDED.metacritic_score,
                    recommendations_total = EXCLUDED.recommendations_total,
                    required_age = EXCLUDED.required_age,
                    updated_at = now()
            """, prepared['games'])
            
            print("  Inserting genres...")
            cur.executemany("""
                INSERT INTO genres (genre_id, name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, prepared['genres'])
            
            print("  Inserting categories...")
            cur.executemany("""
                INSERT INTO categories (category_id, name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, prepared['categories'])
            
            print("  Inserting developers...")
            dev_tuples = [(name,) for name in prepared['developers']]
            cur.executemany("""
                INSERT INTO developers (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
            """, dev_tuples)
            
            print("  Inserting publishers...")
            pub_tuples = [(name,) for name in prepared['publishers']]
            cur.executemany("""
                INSERT INTO publishers (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
            """, pub_tuples)
            
            print("  Building ID lookups...")
            cur.execute("SELECT developer_id, name FROM developers")
            dev_ids = {name: did for did, name in cur.fetchall()}
            
            cur.execute("SELECT publisher_id, name FROM publishers")
            pub_ids = {name: pid for pid, name in cur.fetchall()}
            
            print("  Inserting game-genres...")
            cur.executemany("""
                INSERT INTO game_genres (appid, genre_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, prepared['game_genres'])
            
            print("  Inserting game-categories...")
            cur.executemany("""
                INSERT INTO game_categories (appid, category_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, prepared['game_categories'])
            
            print("  Inserting game-developers...")
            game_dev_rows = [(appid, dev_ids[name]) for appid, name in prepared['game_developers']]
            cur.executemany("""
                INSERT INTO game_developers (appid, developer_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, game_dev_rows)
            
            print("  Inserting game-publishers...")
            game_pub_rows = [(appid, pub_ids[name]) for appid, name in prepared['game_publishers']]
            cur.executemany("""
                INSERT INTO game_publishers (appid, publisher_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, game_pub_rows)
            
            print("  Committing...")
        
        conn.commit()
        print(f"  Done - {len(prepared['games'])} games")


def main():
    checkpoint = CheckpointManager(Path(PATH))
    data = checkpoint.load(FILENAME)
    
    print(f"Loaded {len(data)} entries")
    insert_batch(data)


if __name__ == "__main__":
    main()