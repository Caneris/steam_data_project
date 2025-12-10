from steam_api_scraper import CheckpointManager
from pathlib import Path
import json

# path to test data
FILENAME = "apps_data"
PATH = "checkpoints"

# Load test data
checkpoint = CheckpointManager(Path(PATH))
data = checkpoint.load(FILENAME)

print(f"Loaded {len(data)} entries from test data.")
for appid, appdata in list(data.items())[:2]:  # print first 2 entries
    print(f"AppID: {appid}, Data: {appdata}")

# save as json for easier inspection
# add test_output directory
output_path = Path("test_output")
output_path.mkdir(exist_ok=True)
with open(output_path / f"{FILENAME}.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

