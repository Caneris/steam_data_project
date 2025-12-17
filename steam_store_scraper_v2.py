"""
Steam Store Page Scraper

Scrapes additional data from Steam store pages that isn't available via API:
- User-defined tags with vote counts
- Review counts by language
- Review scores by language

Usage:
    python steam_store_scraper.py                    # Full run
    python steam_store_scraper.py --test             # Test with 20 apps
    python steam_store_scraper.py --test 50          # Test with 50 apps
"""

import json
import os
import re
import time
import requests
import psycopg
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from contextlib import contextmanager
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from checkpoint import CheckpointManager, BaseCheckpoint

load_dotenv()


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


# ============ Configuration ============

@dataclass
class ScraperConfig:
    checkpoint_dir: Path = Path("checkpoints")
    output_file: Path = Path("data/steam_store_raw.jsonl")
    min_reviews: int = 0  # Minimum recommendations_total to include
    
    # Rate limiting - be respectful to Steam
    requests_per_minute: int = 50  # ~2 seconds between requests
    checkpoint_interval: int = 500
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 60
    
    # Testing
    test_mode: bool = False
    test_limit: int = 20


# ============ Checkpoint Definition ============

@dataclass
class StorePageCheckpoint(BaseCheckpoint):
    """Tracks progress for Steam store page scraping."""
    app_ids_to_scrape: list[int] = field(default_factory=list)
    completed_ids: set[int] = field(default_factory=set)
    no_data_ids: set[int] = field(default_factory=set)  # Page exists but no usable data
    failed_ids: dict[int, str] = field(default_factory=dict)  # appid -> error
    
    @property
    def total(self) -> int:
        return len(self.app_ids_to_scrape)
    
    @property
    def processed(self) -> int:
        return len(self.completed_ids) + len(self.no_data_ids) + len(self.failed_ids)
    
    @property
    def remaining(self) -> int:
        return self.total - self.processed
    
    def get_pending(self) -> list[int]:
        """Return app IDs that haven't been processed yet."""
        done = self.completed_ids | self.no_data_ids | set(self.failed_ids.keys())
        return [appid for appid in self.app_ids_to_scrape if appid not in done]
    
    def summary(self) -> str:
        return (
            f"Completed: {len(self.completed_ids)}, "
            f"NoData: {len(self.no_data_ids)}, "
            f"Failed: {len(self.failed_ids)}"
        )


# ============ Logging ============

def log(message: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


# ============ Store Page Parser ============

class StorePageParser:
    """Parses Steam store page HTML to extract tags, reviews by language."""
    
    # Regex to find the InitAppTagModal JavaScript call with tag data
    TAG_MODAL_PATTERN = re.compile(
        r'InitAppTagModal\s*\(\s*\d+\s*,\s*(\[.*?\])\s*,',
        re.DOTALL
    )
    
    # Review score mapping
    REVIEW_SCORES = {
        'Overwhelmingly Positive': 9,
        'Very Positive': 8,
        'Positive': 7,
        'Mostly Positive': 6,
        'Mixed': 5,
        'Mostly Negative': 4,
        'Negative': 3,
        'Very Negative': 2,
        'Overwhelmingly Negative': 1,
    }
    
    def parse(self, html: str) -> dict | None:
        """
        Parse store page HTML and extract all data points.
        
        Returns:
            dict with 'tags', 'reviews_by_language' keys, or None if parsing fails
        """
        result = {
            'tags': self._parse_tags(html),
            'reviews_by_language': self._parse_reviews_by_language(html),
        }
        
        # Consider it valid if we got at least one data point
        if result['tags'] or result['reviews_by_language']:
            return result
        
        return None
    
    def _parse_tags(self, html: str) -> list[dict] | None:
        """
        Extract tags with vote counts from InitAppTagModal JavaScript.
        
        Returns list of dicts: [{"tagid": 122, "name": "RPG", "count": 2021}, ...]
        """
        match = self.TAG_MODAL_PATTERN.search(html)
        if not match:
            return None
        
        try:
            tags_json = match.group(1)
            tags = json.loads(tags_json)
            
            # Clean up - only keep fields we need
            return [
                {
                    'tagid': tag['tagid'],
                    'name': tag['name'],
                    'count': tag['count']
                }
                for tag in tags
                if isinstance(tag, dict) and 'tagid' in tag
            ]
        except (json.JSONDecodeError, KeyError, TypeError):
            return None
    
    def _parse_reviews_by_language(self, html: str) -> list[dict] | None:
        """
        Extract review counts and scores by language from the filter dropdown.
        
        Returns list of dicts: [{"language": "english", "language_name": "English", 
                                  "count": 19306, "score": "Very Positive", "score_numeric": 8}, ...]
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Find the language filter dropdown
        language_dropdown = soup.find('div', id='review_language_flyout')
        if not language_dropdown:
            return None
        
        reviews = []
        
        # Find all language radio inputs with associated labels
        # Look for inputs in the outliers dropdown (languages with enough reviews)
        outliers_div = language_dropdown.find('div', class_='user_reviews_language_outliers_dropdown')
        if not outliers_div:
            return None
        
        for radio in outliers_div.find_all('input', {'type': 'radio', 'name': 'review_language'}):
            language_code = radio.get('value')
            if not language_code:
                continue
            
            # Get the label for this radio
            label = radio.find_next('label', {'for': radio.get('id')})
            if not label:
                continue
            
            # Extract language display name from data attribute
            language_name = radio.get('data-language', language_code)
            
            # Extract review score
            score_span = label.find('span', class_='game_review_summary')
            score_text = score_span.get_text(strip=True) if score_span else None
            score_numeric = self.REVIEW_SCORES.get(score_text) if score_text else None
            
            # Extract review count
            count_span = label.find('span', class_='user_reviews_count')
            count = None
            if count_span:
                count_text = count_span.get_text(strip=True)
                # Parse "(19,306)" -> 19306
                count_match = re.search(r'([\d,]+)', count_text)
                if count_match:
                    count = int(count_match.group(1).replace(',', ''))
            
            if count is not None:  # Only add if we got a count
                reviews.append({
                    'language': language_code,
                    'language_name': language_name,
                    'count': count,
                    'score': score_text,
                    'score_numeric': score_numeric,
                })
        
        return reviews if reviews else None


# ============ Steam Store API ============

class SteamStoreAPI:
    """Fetches Steam store pages."""
    
    BASE_URL = "https://store.steampowered.com/app"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        # Bypass age gate and mature content warnings
        "Cookie": "birthtime=0; wants_mature_content=1; lastagecheckage=1-0-1990; mature_content=1"
    }
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.parser = StorePageParser()
    
    def get_store_data(self, appid: int) -> dict | None:
        """
        Fetch and parse store page for a single app.
        
        Returns:
            dict with parsed data if successful
            None if page not found or parsing failed
            
        Raises:
            requests.RequestException on unrecoverable network errors
        """
        url = f"{self.BASE_URL}/{appid}"
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, timeout=self.config.timeout)
                
                if response.status_code == 200:
                    return self.parser.parse(response.text)
                
                elif response.status_code == 404:
                    return None
                
                elif response.status_code == 429:
                    log(f"Rate limited, waiting {self.config.retry_delay}s...")
                    time.sleep(self.config.retry_delay)
                    continue
                
                elif response.status_code == 403:
                    log(f"Forbidden on appid {appid}, waiting 5 min...")
                    time.sleep(300)
                    continue
                
                else:
                    log(f"HTTP {response.status_code} for appid {appid}")
                    return None
                    
            except requests.exceptions.Timeout:
                log(f"Timeout on appid {appid}, attempt {attempt + 1}/{self.config.max_retries}")
                time.sleep(10)
                continue
            
            except requests.exceptions.RequestException as e:
                log(f"Request error on appid {appid}: {e}")
                raise
        
        raise requests.exceptions.RequestException(f"Max retries exceeded for appid {appid}")


# ============ Scraper ============

class SteamStoreScraper:
    """Main scraper class for collecting Steam store page data."""
    
    CHECKPOINT_NAME = "steam_store"
    
    def __init__(self, config: ScraperConfig, app_ids: list[int] | None = None):
        self.config = config
        self.api = SteamStoreAPI(config)
        self.checkpoint_mgr = CheckpointManager(config.checkpoint_dir)
        self.delay = 60 / config.requests_per_minute
        
        # Ensure output directory exists
        self.config.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load or create checkpoint
        self.checkpoint = self._load_checkpoint(app_ids)
    
    def _load_checkpoint(self, app_ids: list[int] | None) -> StorePageCheckpoint:
        """Load existing checkpoint or create a new one."""
        if self.config.test_mode:
            log("Test mode: starting fresh checkpoint")
            checkpoint = StorePageCheckpoint()
            if app_ids:
                checkpoint.app_ids_to_scrape = app_ids
            return checkpoint
        
        existing = self.checkpoint_mgr.load(self.CHECKPOINT_NAME, StorePageCheckpoint)
        if existing:
            log(f"Resumed checkpoint: {existing.summary()}, {existing.remaining} remaining")
            return existing
        
        log("No existing checkpoint, starting fresh")
        checkpoint = StorePageCheckpoint()
        if app_ids:
            checkpoint.app_ids_to_scrape = app_ids
        return checkpoint
    
    def _save_checkpoint(self) -> None:
        """Save current progress."""
        self.checkpoint_mgr.save(self.CHECKPOINT_NAME, self.checkpoint)
        log(f"Checkpoint saved: {self.checkpoint.summary()}")
    
    def _append_result(self, appid: int, data: dict) -> None:
        """Append a single result to the JSONL output file."""
        record = {
            "appid": appid,
            "scraped_at": datetime.now().isoformat(),
            "data": data
        }
        with open(self.config.output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record) + '\n')
    
    def load_app_ids_from_database(self) -> bool:
        """
        Load app IDs from the database, filtered by minimum review count.
        
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT appid FROM games 
                        WHERE recommendations_total > %s
                        ORDER BY appid
                    """, (self.config.min_reviews,))
                    
                    app_ids = [row[0] for row in cur.fetchall()]
            
            self.checkpoint.app_ids_to_scrape = app_ids
            log(f"Loaded {len(app_ids)} app IDs from database (min reviews: {self.config.min_reviews})")
            return True
        
        except Exception as e:
            log(f"Database error: {e}")
            return False
    
    def run(self):
        """Main scraping loop."""
        if self.config.test_mode:
            log(f"=== TEST MODE: Limited to {self.config.test_limit} apps ===")
        
        log("Starting Steam store page scraper...")
        self.checkpoint.mark_started()
        
        # Load app IDs if not already set
        if not self.checkpoint.app_ids_to_scrape:
            if not self.load_app_ids_from_database():
                log("No app IDs to scrape. Check database connection.")
                return
        
        # Get pending apps
        pending = self.checkpoint.get_pending()
        
        if not pending:
            log("All apps already processed!")
            self._print_summary()
            return
        
        # Limit for test mode
        if self.config.test_mode:
            pending = pending[:self.config.test_limit]
            log(f"Test mode: limiting to {len(pending)} apps")
        
        total = len(pending)
        log(f"Starting scrape of {total} apps...")
        start_time = time.time()
        
        try:
            for i, appid in enumerate(pending, 1):
                try:
                    data = self.api.get_store_data(appid)
                    
                    if data:
                        self._append_result(appid, data)
                        self.checkpoint.completed_ids.add(appid)
                    else:
                        self.checkpoint.no_data_ids.add(appid)
                
                except requests.RequestException as e:
                    self.checkpoint.failed_ids[appid] = str(e)
                    log(f"Failed: {appid} - {e}")
                
                # Progress logging
                if i % 50 == 0 or i == total:
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60 if elapsed > 0 else 0
                    eta = (total - i) / (rate / 60) if rate > 0 else 0
                    
                    log(
                        f"Progress: {i}/{total} ({i/total*100:.1f}%) | "
                        f"Rate: {rate:.1f}/min | "
                        f"ETA: {eta/60:.1f} min"
                    )
                
                # Checkpoint save
                if i % self.config.checkpoint_interval == 0:
                    self._save_checkpoint()
                
                # Rate limiting
                time.sleep(self.delay)
        
        except KeyboardInterrupt:
            log("Interrupted by user, saving progress...")
        
        finally:
            self._save_checkpoint()
            self._print_summary()
    
    def _print_summary(self):
        """Print final statistics."""
        log("=" * 50)
        log("SCRAPING SUMMARY")
        log("=" * 50)
        log(f"Total apps:    {self.checkpoint.total}")
        log(f"Completed:     {len(self.checkpoint.completed_ids)}")
        log(f"No data:       {len(self.checkpoint.no_data_ids)}")
        log(f"Failed:        {len(self.checkpoint.failed_ids)}")
        log(f"Remaining:     {self.checkpoint.remaining}")
        
        if self.checkpoint.started_at:
            elapsed = datetime.now() - self.checkpoint.started_at
            log(f"Runtime:       {elapsed}")
    
    def get_stats(self) -> dict:
        """Load completed data and return statistics."""
        if not self.config.output_file.exists():
            return {"error": "No output file found"}
        
        from collections import Counter
        
        all_tags = []
        all_languages = []
        app_count = 0
        
        with open(self.config.output_file, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                app_count += 1
                
                # Count tags
                tags = record.get('data', {}).get('tags', [])
                if tags:
                    for tag in tags:
                        all_tags.append(tag['name'])
                
                # Count languages
                reviews = record.get('data', {}).get('reviews_by_language', [])
                if reviews:
                    for review in reviews:
                        all_languages.append(review['language_name'])
        
        tag_counts = Counter(all_tags)
        language_counts = Counter(all_languages)
        
        return {
            "apps_with_data": app_count,
            "unique_tags": len(tag_counts),
            "top_20_tags": tag_counts.most_common(20),
            "languages_found": len(language_counts),
            "top_languages": language_counts.most_common(10),
        }


# ============ Main ============

def parse_args():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Scrape tags and review data from Steam store pages"
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("checkpoints"),
        help="Directory for checkpoint files (default: checkpoints)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/steam_store_raw.jsonl"),
        help="Output JSONL file (default: data/steam_store_raw.jsonl)"
    )
    parser.add_argument(
        "--min-reviews",
        type=int,
        default=0,
        help="Minimum review count to include (default: 0, meaning > 0 reviews)"
    )
    parser.add_argument(
        "--test",
        nargs="?",
        const=20,
        type=int,
        metavar="N",
        help="Test mode: only scrape N apps (default: 20)"
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    config = ScraperConfig(
        checkpoint_dir=args.checkpoint_dir,
        output_file=args.output,
        min_reviews=args.min_reviews,
        test_mode=args.test is not None,
        test_limit=args.test if args.test else 20
    )
    
    scraper = SteamStoreScraper(config)
    scraper.run()
    
    # Show stats if we have data
    if not config.test_mode and scraper.checkpoint.completed_ids:
        stats = scraper.get_stats()
        if "error" not in stats:
            log("\nTop 20 tags:")
            for tag, count in stats.get("top_20_tags", []):
                log(f"  {tag}: {count}")
            
            log("\nTop languages by review count:")
            for lang, count in stats.get("top_languages", []):
                log(f"  {lang}: {count}")


if __name__ == "__main__":
    main()