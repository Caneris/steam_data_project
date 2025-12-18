"""
SteamSpy API Scraper

Scrapes game data from SteamSpy API and saves raw JSON responses
to a JSONL file for later processing and database loading.

SteamSpy API: https://steamspy.com/api.php
Rate limit: 1 request per second (we use 1.2s to be safe)

Usage:
    python steamspy_scraper.py                    # Full run
    python steamspy_scraper.py --test             # Test with 20 apps
    python steamspy_scraper.py --test 50          # Test with 50 apps
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field

from checkpoint import CheckpointManager, BaseCheckpoint


# ============ Configuration ============

@dataclass
class ScraperConfig:
    checkpoint_dir: Path = Path("checkpoints")
    steam_checkpoint_file: Path = Path("checkpoints/apps_data.pkl")
    output_file: Path = Path("data/steamspy_raw.jsonl")
    request_delay: float = 1.2  # seconds (API limit is 1/sec)
    checkpoint_interval: int = 100
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 60
    test_mode: bool = False
    test_limit: int = 20


# ============ Checkpoint Definition ============

@dataclass
class SteamSpyCheckpoint(BaseCheckpoint):
    """Tracks progress for SteamSpy scraping."""
    app_ids_to_scrape: list[int] = field(default_factory=list)
    completed_ids: set[int] = field(default_factory=set)
    no_data_ids: set[int] = field(default_factory=set)
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


# ============ SteamSpy API ============

class SteamSpyAPI:
    BASE_URL = "https://steamspy.com/api.php"
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Steam Analytics Project (educational/portfolio)'
        })
    
    def get_app_details(self, appid: int) -> dict | None:
        """
        Fetch a single app's data from SteamSpy.
        
        Returns:
            dict with app data if successful
            None if no data available (non-game, removed, etc.)
            
        Raises:
            requests.RequestException on unrecoverable network errors
        """
        params = {'request': 'appdetails', 'appid': appid}
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.config.timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # SteamSpy returns minimal data for non-existent apps
                    if not data or data.get('name') in (None, '', 'None'):
                        return None
                    return data
                
                elif response.status_code == 429:
                    log(f"Rate limited, waiting {self.config.retry_delay}s...")
                    time.sleep(self.config.retry_delay)
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

class SteamSpyScraper:
    CHECKPOINT_NAME = "steamspy"
    
    def __init__(self, config: ScraperConfig, app_ids: list[int] | None = None):
        self.config = config
        self.api = SteamSpyAPI(config)
        self.checkpoint_mgr = CheckpointManager(config.checkpoint_dir)
        
        # Ensure output directory exists
        self.config.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load or create checkpoint
        self.checkpoint = self._load_checkpoint(app_ids)
    
    def _load_checkpoint(self, app_ids: list[int] | None) -> SteamSpyCheckpoint:
        """Load existing checkpoint or create a new one."""
        if self.config.test_mode:
            log("Test mode: starting fresh checkpoint")
            checkpoint = SteamSpyCheckpoint()
            if app_ids:
                checkpoint.app_ids_to_scrape = app_ids
            return checkpoint
        
        existing = self.checkpoint_mgr.load(self.CHECKPOINT_NAME, SteamSpyCheckpoint)
        if existing:
            log(f"Resumed checkpoint: {existing.summary()}, {existing.remaining} remaining")
            return existing
        
        log("No existing checkpoint, starting fresh")
        checkpoint = SteamSpyCheckpoint()
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
    
    def load_app_ids_from_steam_checkpoint(self) -> bool:
        """
        Load app IDs from the Steam API scraper's checkpoint file.
        
        Returns:
            True if loaded successfully, False otherwise
        """
        import pickle
        
        path = self.config.steam_checkpoint_file
        
        if not path.exists():
            log(f"Steam checkpoint file not found: {path}")
            return False
        
        with open(path, 'rb') as f:
            steam_checkpoint = pickle.load(f)
        
        # Handle both old dict format and new dataclass format
        if hasattr(steam_checkpoint, 'apps_data'):
            app_ids = list(steam_checkpoint.apps_data.keys())
        elif isinstance(steam_checkpoint, dict):
            app_ids = list(steam_checkpoint.keys())
        else:
            log(f"Unknown checkpoint format: {type(steam_checkpoint)}")
            return False
        
        self.checkpoint.app_ids_to_scrape = app_ids
        log(f"Loaded {len(app_ids)} app IDs from {path}")
        return True
    
    def run(self):
        """Main scraping loop."""
        if self.config.test_mode:
            log(f"=== TEST MODE: Limited to {self.config.test_limit} apps ===")
        
        log("Starting SteamSpy scraper...")
        self.checkpoint.mark_started()
        
        # Load app IDs if not already set
        if not self.checkpoint.app_ids_to_scrape:
            if not self.load_app_ids_from_steam_checkpoint():
                log("No app IDs to scrape. Run Steam API scraper first or provide IDs.")
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
                    data = self.api.get_app_details(appid)
                    
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
                time.sleep(self.config.request_delay)
        
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
        app_count = 0
        
        with open(self.config.output_file, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                app_count += 1
                tags = record.get('data', {}).get('tags', {})
                all_tags.extend(tags.keys())
        
        tag_counts = Counter(all_tags)
        
        return {
            "apps_with_data": app_count,
            "unique_tags": len(tag_counts),
            "top_20_tags": tag_counts.most_common(20),
            "avg_tags_per_app": len(all_tags) / app_count if app_count else 0
        }


# ============ Main ============

def parse_args():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Scrape game data from SteamSpy API"
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("checkpoints"),
        help="Directory for checkpoint files (default: checkpoints)"
    )
    parser.add_argument(
        "--steam-checkpoint",
        type=Path,
        default=Path("checkpoints/apps_data.pkl"),
        help="Path to Steam API checkpoint pickle file (default: checkpoints/apps_data.pkl)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/steamspy_raw.jsonl"),
        help="Output JSONL file (default: data/steamspy_raw.jsonl)"
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
        steam_checkpoint_file=args.steam_checkpoint,
        output_file=args.output,
        test_mode=args.test is not None,
        test_limit=args.test if args.test else 20
    )
    
    scraper = SteamSpyScraper(config)
    scraper.run()
    
    # Show tag stats if we have data
    if not config.test_mode and scraper.checkpoint.completed_ids:
        stats = scraper.get_stats()
        if "error" not in stats:
            log("\nTop 20 tags:")
            for tag, count in stats.get("top_20_tags", []):
                log(f"  {tag}: {count}")


if __name__ == "__main__":
    main()