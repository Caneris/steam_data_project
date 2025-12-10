import os
import time
import requests
import pickle
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# ============ Configuration ============

@dataclass
class ScraperConfig:
    steam_api_key: str = field(default_factory=lambda: os.getenv("STEAM_API_KEY"))
    checkpoint_dir: Path = Path("checkpoints")
    requests_per_5min: int = 200
    checkpoint_interval: int = 2500
    test_mode: bool = False
    test_limit: int = 20

# ============ Logging ============

def log(message: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# ============ Checkpoint Management ============

class CheckpointManager:
    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
    
    def save(self, data: dict, filename: str):
        path = self.directory / f"{filename}.pkl"
        with open(path, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        log(f"Saved checkpoint: {path}")
    
    def load(self, filename: str) -> dict | None:
        path = self.directory / f"{filename}.pkl"
        if path.exists():
            with open(path, "rb") as f:
                log(f"Loaded checkpoint: {path}")
                return pickle.load(f)
        return None

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
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.api = SteamAPI(config.steam_api_key)
        self.checkpoint = CheckpointManager(config.checkpoint_dir)
        self.delay = (5 * 60) / config.requests_per_5min
        
        # Data storage
        self.apps_data: dict = {}
        self.excluded_apps: set = set()
        self.error_apps: set = set()
    
    def load_progress(self):
        """Load previous progress from checkpoints"""
        if self.config.test_mode:
            log("Test mode: skipping checkpoint loading")
            return
            
        self.apps_data = self.checkpoint.load("apps_data") or {}
        self.excluded_apps = self.checkpoint.load("excluded_apps") or set()
        self.error_apps = self.checkpoint.load("error_apps") or set()
        
        log(f"Progress: {len(self.apps_data)} scraped, {len(self.excluded_apps)} excluded, {len(self.error_apps)} errors")
    
    def save_progress(self):
        """Save current progress to checkpoints"""
        if self.config.test_mode:
            log("Test mode: skipping checkpoint saving")
            log("Final test data:")
            log(f"  Scraped: {len(self.apps_data)}")
            log(f"  Excluded: {len(self.excluded_apps)}")
            log(f"  Errors: {len(self.error_apps)}")
            self.checkpoint.save(self.apps_data, "apps_data")
            return
            
        self.checkpoint.save(self.apps_data, "apps_data")
        self.checkpoint.save(self.excluded_apps, "excluded_apps")
        self.checkpoint.save(self.error_apps, "error_apps")
    
    def get_remaining_apps(self, all_apps: list[dict]) -> list[int]:
        """Filter out already processed apps"""
        all_ids = {app["appid"] for app in all_apps}
        processed = set(self.apps_data.keys()) | self.excluded_apps | self.error_apps
        remaining = all_ids - processed
        return list(remaining)
    
    def run(self):
        """Main scraping loop"""
        if self.config.test_mode:
            log(f"=== TEST MODE: Limited to {self.config.test_limit} apps ===")
        
        log("Starting Steam scraper...")
        
        # Load previous progress
        self.load_progress()
        
        # Get all app IDs
        log("Fetching app list...")
        all_apps = self.api.get_all_app_ids()
        log(f"Total apps on Steam: {len(all_apps)}")
        
        # Filter to remaining
        remaining = self.get_remaining_apps(all_apps)
        log(f"Remaining to scrape: {len(remaining)}")
        
        # Limit for test mode
        if self.config.test_mode:
            remaining = remaining[:self.config.test_limit]
            log(f"Test mode: limiting to {len(remaining)} apps")
        
        # Scrape loop
        for i, appid in enumerate(remaining):
            details = self.api.get_app_details(appid)
            
            if details is None:
                self.error_apps.add(appid)
            elif not details.get("success", False):
                self.excluded_apps.add(appid)
                log(f"App {appid} not available")
            else:
                self.apps_data[appid] = details["data"]
                log(f"Scraped app {appid}: {details['data'].get('name', 'Unknown')}")
            
            # Checkpoint every N requests (not in test mode)
            if not self.config.test_mode and (i + 1) % self.config.checkpoint_interval == 0:
                self.save_progress()
            
            time.sleep(self.delay)
        
        # Final save
        self.save_progress()
        
        # Summary
        log("=== Summary ===")
        log(f"Scraped: {len(self.apps_data)}")
        log(f"Excluded: {len(self.excluded_apps)}")
        log(f"Errors: {len(self.error_apps)}")
        
        if self.config.test_mode:
            log("=== Test data preview ===")
            for appid, data in list(self.apps_data.items())[:3]:
                log(f"  {appid}: {data.get('name')} - {data.get('type')}")

# ============ Main ============

def main():
    config = ScraperConfig(
        test_mode=True,   # Set to False for full run
        test_limit=10
    )
    scraper = SteamScraper(config)
    scraper.run()

if __name__ == "__main__":
    main()