"""
Steam Store Tags Scraper
========================
Scrapes user-defined tags from Steam store pages.
Designed to run after app details scraping is complete.

Usage:
    python steam_tags_scraper.py
    
Set test_mode=True in config for a quick validation run.
"""

import os
import time
import requests
import pickle
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ============ Configuration ============

@dataclass
class TagScraperConfig:
    checkpoint_dir: Path = Path("checkpoints")
    output_dir: Path = Path("data")
    
    # Rate limiting - be respectful to Steam
    requests_per_minute: int = 30  # ~2 seconds between requests
    checkpoint_interval: int = 500
    
    # Testing
    test_mode: bool = False
    test_limit: int = 20
    
    # Request settings
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 60  # seconds to wait after rate limit


# ============ Logging ============

def log(message: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


# ============ Checkpoint Management ============

class CheckpointManager:
    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
    
    def save(self, data, filename: str):
        path = self.directory / f"{filename}.pkl"
        with open(path, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        log(f"Saved checkpoint: {path}")
    
    def load(self, filename: str):
        path = self.directory / f"{filename}.pkl"
        if path.exists():
            with open(path, "rb") as f:
                log(f"Loaded checkpoint: {path}")
                return pickle.load(f)
        return None


# ============ Steam Store Scraper ============

class SteamStoreScraper:
    """Scrapes data from Steam store pages (HTML)"""
    
    BASE_URL = "https://store.steampowered.com/app"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        # Bypass age gate and mature content warnings
        "Cookie": "birthtime=0; wants_mature_content=1; lastagecheckage=1-0-1990; mature_content=1"
    }
    
    def __init__(self, config: TagScraperConfig):
        self.config = config
        self.delay = 60 / config.requests_per_minute
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def get_tags(self, appid: int) -> list[str] | None:
        """
        Fetch user-defined tags for a single app.
        
        Returns:
            list[str]: List of tags if successful
            None: If page not found or error occurred
        """
        url = f"{self.BASE_URL}/{appid}"
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, timeout=self.config.timeout)
                
                if response.status_code == 200:
                    return self._parse_tags(response.text)
                
                elif response.status_code == 404:
                    # App doesn't exist or was removed
                    return None
                
                elif response.status_code == 429:
                    log(f"Rate limited on appid {appid}, waiting {self.config.retry_delay}s...")
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
                return None
        
        log(f"Max retries exceeded for appid {appid}")
        return None
    
    def _parse_tags(self, html: str) -> list[str]:
        """Extract tags from Steam store page HTML"""
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all app_tag anchor elements
        tag_elements = soup.select("a.app_tag")
        
        tags = [tag.get_text(strip=True) for tag in tag_elements]
        
        return tags


# ============ Main Scraper ============

class SteamTagsScraper:
    """
    Main scraper class for collecting Steam tags.
    Integrates with checkpoint system for resumable scraping.
    """
    
    def __init__(self, config: TagScraperConfig):
        self.config = config
        self.scraper = SteamStoreScraper(config)
        self.checkpoint = CheckpointManager(config.checkpoint_dir)
        
        # Ensure output directory exists
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Data storage
        self.tags_data: dict[int, list[str]] = {}  # appid -> list of tags
        self.no_tags_apps: set[int] = set()  # apps with no tags (but page exists)
        self.error_apps: set[int] = set()  # apps that errored or don't exist
    
    def load_progress(self):
        """Load previous progress from checkpoints"""
        self.tags_data = self.checkpoint.load("tags_data") or {}
        self.no_tags_apps = self.checkpoint.load("no_tags_apps") or set()
        self.error_apps = self.checkpoint.load("error_apps") or set()
        
        log(f"Loaded progress: {len(self.tags_data)} with tags, "
            f"{len(self.no_tags_apps)} without tags, {len(self.error_apps)} errors")
    
    def save_progress(self):
        """Save current progress to checkpoints"""
        self.checkpoint.save(self.tags_data, "tags_data")
        self.checkpoint.save(self.no_tags_apps, "no_tags_apps")
        self.checkpoint.save(self.error_apps, "error_apps")
    
    def load_appids_from_checkpoint(self) -> list[int]:
        """
        Load appids from existing apps_data checkpoint (from app details scraper).
        Falls back to a manual list if checkpoint doesn't exist.
        """
        apps_data = self.checkpoint.load("apps_data")
        
        if apps_data:
            # apps_data is dict with appid keys
            appids = list(apps_data.keys())
            log(f"Loaded {len(appids)} appids from apps_data checkpoint")
            return appids
        
        log("WARNING: No apps_data checkpoint found!")
        log("Please run the app details scraper first, or provide appids manually.")
        return []
    
    def get_pending_appids(self, all_appids: list[int]) -> list[int]:
        """Filter out already processed appids"""
        processed = set(self.tags_data.keys()) | self.no_tags_apps | self.error_apps
        pending = [appid for appid in all_appids if appid not in processed]
        return pending
    
    def run(self, appids: list[int] | None = None):
        """
        Main scraping loop.
        
        Args:
            appids: Optional list of appids to scrape. If None, loads from checkpoint.
        """
        # Load previous progress
        self.load_progress()
        
        # Get appids to process
        if appids is None:
            appids = self.load_appids_from_checkpoint()
        
        if not appids:
            log("No appids to process. Exiting.")
            return
        
        # Filter to pending only
        pending = self.get_pending_appids(appids)
        
        # Apply test limit if in test mode
        if self.config.test_mode:
            pending = pending[:self.config.test_limit]
            log(f"TEST MODE: Processing only {len(pending)} apps")
        
        total = len(pending)
        log(f"Starting scrape: {total} apps to process")
        
        if total == 0:
            log("All apps already processed!")
            return
        
        start_time = time.time()
        
        for i, appid in enumerate(pending, 1):
            # Fetch tags
            tags = self.scraper.get_tags(appid)
            
            # Categorize result
            if tags is None:
                self.error_apps.add(appid)
            elif len(tags) == 0:
                self.no_tags_apps.add(appid)
            else:
                self.tags_data[appid] = tags
            
            # Progress logging
            if i % 50 == 0 or i == total:
                elapsed = time.time() - start_time
                rate = i / elapsed * 60  # apps per minute
                eta_minutes = (total - i) / rate if rate > 0 else 0
                
                log(f"Progress: {i}/{total} ({i/total*100:.1f}%) | "
                    f"Rate: {rate:.1f}/min | "
                    f"ETA: {eta_minutes:.0f} min | "
                    f"Tags: {len(self.tags_data)} | "
                    f"NoTags: {len(self.no_tags_apps)} | "
                    f"Errors: {len(self.error_apps)}")
            
            # Checkpoint save
            if i % self.config.checkpoint_interval == 0:
                self.save_progress()
            
            # Rate limiting
            time.sleep(self.scraper.delay)
        
        # Final save
        self.save_progress()
        
        elapsed = time.time() - start_time
        log(f"Scraping complete! Processed {total} apps in {elapsed/60:.1f} minutes")
        log(f"Final stats: {len(self.tags_data)} with tags, "
            f"{len(self.no_tags_apps)} without tags, {len(self.error_apps)} errors")
    
    def export_to_csv(self, filename: str = "steam_tags.csv"):
        """Export tags data to CSV for easy analysis"""
        import csv
        
        output_path = self.config.output_dir / filename
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["appid", "tags"])
            
            for appid, tags in sorted(self.tags_data.items()):
                # Join tags with pipe separator
                writer.writerow([appid, "|".join(tags)])
        
        log(f"Exported {len(self.tags_data)} apps to {output_path}")
    
    def get_stats(self) -> dict:
        """Return summary statistics"""
        all_tags = []
        for tags in self.tags_data.values():
            all_tags.extend(tags)
        
        from collections import Counter
        tag_counts = Counter(all_tags)
        
        return {
            "total_apps_with_tags": len(self.tags_data),
            "total_apps_no_tags": len(self.no_tags_apps),
            "total_errors": len(self.error_apps),
            "unique_tags": len(tag_counts),
            "top_20_tags": tag_counts.most_common(20),
            "avg_tags_per_app": len(all_tags) / len(self.tags_data) if self.tags_data else 0
        }


# ============ Main Entry Point ============

if __name__ == "__main__":
    # Configuration
    config = TagScraperConfig(
        checkpoint_dir=Path("checkpoints"),
        output_dir=Path("data"),
        requests_per_minute=30,  # ~2 sec delay between requests
        checkpoint_interval=500,
        test_mode=True,  # Set to False for full run
        test_limit=20
    )
    
    # Initialize and run
    scraper = SteamTagsScraper(config)
    scraper.run()
    
    # Show stats
    if scraper.tags_data:
        stats = scraper.get_stats()
        print("\n" + "="*50)
        print("SCRAPING STATS")
        print("="*50)
        print(f"Apps with tags: {stats['total_apps_with_tags']}")
        print(f"Apps without tags: {stats['total_apps_no_tags']}")
        print(f"Errors: {stats['total_errors']}")
        print(f"Unique tags found: {stats['unique_tags']}")
        print(f"Avg tags per app: {stats['avg_tags_per_app']:.1f}")
        print("\nTop 20 tags:")
        for tag, count in stats['top_20_tags']:
            print(f"  {tag}: {count}")
    
    # Export if not in test mode
    if not config.test_mode and scraper.tags_data:
        scraper.export_to_csv() 