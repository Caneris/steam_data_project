"""
Steam App Details Scraper

Scrapes game details from Steam's Store API using the shared checkpoint module.

Usage:
    python steam_api_scraper.py                                             # Full run
    python steam_api_scraper.py --help                                      # Show help
    python steam_api_scraper.py --test                                      # Test with 20 apps
    python steam_api_scraper.py --test 50                                   # Test with 50 apps
    python steam_api_scraper.py --checkpoint-dir my_checkpoints             # Custom checkpoint directory
    python steam_api_scraper.py --checkpoint-dir my_checkpoints --test 10   # Custom dir + test

"""

import os
import time
import requests
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from dotenv import load_dotenv

from checkpoint import CheckpointManager, BaseCheckpoint

load_dotenv()


# ============ Configuration ============

@dataclass
class ScraperConfig:
    steam_api_key: str = field(default_factory=lambda: os.getenv("STEAM_API_KEY"))
    checkpoint_dir: Path = Path("checkpoints")
    requests_per_5min: int = 200
    checkpoint_interval: int = 2000
    test_mode: bool = False
    test_limit: int = 20


# ============ Checkpoint Definition ============

@dataclass
class SteamAPICheckpoint(BaseCheckpoint):
    """Tracks progress for Steam API scraping."""
    apps_data: dict[int, dict] = field(default_factory=dict)
    excluded_apps: set[int] = field(default_factory=set)  # No data available
    error_apps: set[int] = field(default_factory=set)      # Request failed
    
    @property
    def total_processed(self) -> int:
        return len(self.apps_data) + len(self.excluded_apps) + len(self.error_apps)
    
    def get_pending(self, all_app_ids: set[int]) -> list[int]:
        """Return app IDs that haven't been processed yet."""
        processed = set(self.apps_data.keys()) | self.excluded_apps | self.error_apps
        return list(all_app_ids - processed)
    
    def summary(self) -> str:
        return (
            f"Scraped: {len(self.apps_data)}, "
            f"Excluded: {len(self.excluded_apps)}, "
            f"Errors: {len(self.error_apps)}"
        )


# ============ Logging ============

def log(message: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


# ============ Steam API ============

class SteamAPI:
    BASE_URL = "https://api.steampowered.com"
    STORE_URL = "https://store.steampowered.com/api"
    
    def __init__(self, api_key: str, delay: float = 1.5):
        self.api_key = api_key
        self.delay = delay
    
    def get_all_app_ids(self) -> list[dict]:
        """Fetch all apps from IStoreService/GetAppList"""
        apps = []
        last_appid = 0
        
        while True:
            params = {
                "key": self.api_key,
                "include_games": True,
                "include_dlc": False,
                "include_software": False,
                "include_videos": False,
                "include_hardware": False,
                "last_appid": last_appid,
                "max_results": 50000
            }
            
            response = requests.get(
                f"{self.BASE_URL}/IStoreService/GetAppList/v1/",
                params=params
            )
            
            if response.status_code != 200:
                log(f"Failed to fetch app list: {response.status_code}")
                break
            
            data = response.json().get("response", {})
            batch = data.get("apps", [])
            
            if not batch:
                break
            
            apps.extend(batch)
            last_appid = batch[-1]["appid"]
            log(f"Fetched {len(apps)} apps so far...")
            
            if not data.get("have_more_results", False):
                break
            
            time.sleep(1)
        
        return apps
    
    def get_app_details(self, appid: int) -> dict | None:
        """Fetch details for a single app"""
        response = requests.get(
            f"{self.STORE_URL}/appdetails",
            params={"appids": appid}
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get(str(appid))
        elif response.status_code == 429:
            log(f"Rate limited, waiting 60s...")
            time.sleep(60)
            return self.get_app_details(appid)  # retry
        elif response.status_code == 403:
            log(f"Forbidden, waiting 5min...")
            time.sleep(300)
            return self.get_app_details(appid)  # retry
        else:
            log(f"Error {response.status_code} for appid {appid}")
            return None


# ============ Scraper ============

class SteamScraper:
    CHECKPOINT_NAME = "steam_api"
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.api = SteamAPI(config.steam_api_key)
        self.checkpoint_mgr = CheckpointManager(config.checkpoint_dir)
        self.delay = (5 * 60) / config.requests_per_5min
        
        # Load or create checkpoint
        self.checkpoint = self._load_checkpoint()
    
    def _load_checkpoint(self) -> SteamAPICheckpoint:
        """Load existing checkpoint or create a new one."""
        if self.config.test_mode:
            log("Test mode: starting fresh checkpoint")
            return SteamAPICheckpoint()
        
        existing = self.checkpoint_mgr.load(self.CHECKPOINT_NAME, SteamAPICheckpoint)
        if existing:
            log(f"Resumed checkpoint: {existing.summary()}")
            return existing
        
        log("No existing checkpoint, starting fresh")
        return SteamAPICheckpoint()
    
    def _save_checkpoint(self) -> None:
        """Save current progress."""
        self.checkpoint_mgr.save(self.CHECKPOINT_NAME, self.checkpoint)
        log(f"Checkpoint saved: {self.checkpoint.summary()}")
    
    def run(self):
        """Main scraping loop"""
        if self.config.test_mode:
            log(f"=== TEST MODE: Limited to {self.config.test_limit} apps ===")
        
        log("Starting Steam scraper...")
        self.checkpoint.mark_started()
        
        # Get all app IDs
        log("Fetching app list...")
        all_apps = self.api.get_all_app_ids()
        all_app_ids = {app["appid"] for app in all_apps}
        log(f"Total apps on Steam: {len(all_app_ids)}")
        
        # Filter to remaining
        remaining = self.checkpoint.get_pending(all_app_ids)
        log(f"Remaining to scrape: {len(remaining)}")
        
        if not remaining:
            log("All apps already processed!")
            return
        
        # Limit for test mode
        if self.config.test_mode:
            remaining = remaining[:self.config.test_limit]
            log(f"Test mode: limiting to {len(remaining)} apps")
        
        # Scrape loop
        try:
            for i, appid in enumerate(remaining, 1):
                details = self.api.get_app_details(appid)
                
                if details is None:
                    self.checkpoint.error_apps.add(appid)
                elif not details.get("success", False):
                    self.checkpoint.excluded_apps.add(appid)
                else:
                    self.checkpoint.apps_data[appid] = details["data"]
                    log(f"Scraped app {appid}: {details['data'].get('name', 'Unknown')}")
                
                # Checkpoint every N requests
                if i % self.config.checkpoint_interval == 0:
                    self._save_checkpoint()
                
                time.sleep(self.delay)
        
        except KeyboardInterrupt:
            log("Interrupted by user, saving progress...")
        
        finally:
            # Always save on exit
            self._save_checkpoint()
            self._print_summary()
    
    def _print_summary(self):
        """Print final statistics."""
        log("=== Summary ===")
        log(f"Scraped: {len(self.checkpoint.apps_data)}")
        log(f"Excluded: {len(self.checkpoint.excluded_apps)}")
        log(f"Errors: {len(self.checkpoint.error_apps)}")
        
        if self.checkpoint.started_at:
            elapsed = datetime.now() - self.checkpoint.started_at
            log(f"Runtime: {elapsed}")
        
        if self.config.test_mode:
            log("=== Test data preview ===")
            for appid, data in list(self.checkpoint.apps_data.items())[:3]:
                log(f"  {appid}: {data.get('name')} - {data.get('type')}")


# ============ Main ============

def parse_args():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Scrape game details from Steam's Store API"
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("checkpoints"),
        help="Directory for checkpoint files (default: checkpoints)"
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
        test_mode=args.test is not None,
        test_limit=args.test if args.test else 20
    )
    
    scraper = SteamScraper(config)
    scraper.run()


if __name__ == "__main__":
    main()