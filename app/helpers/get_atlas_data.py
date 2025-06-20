import requests
import json
import time
from pathlib import Path
from typing import Dict, Any

# Create cache directory if it doesn't exist
cache_dir = Path("./cache")
cache_dir.mkdir(exist_ok=True)

def fetch_square_data(ykj_n: int, ykj_e: int) -> Dict[str, Any]:
    """Fetch species data for a square from the atlas API."""
    url = f"https://atlas-api.2.rahtiapp.fi/api/v1/grid/{ykj_n}:{ykj_e}/atlas"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_cached_square_data(ykj_n: int, ykj_e: int) -> Dict[str, Any]:
    """Get square data from cache or fetch it if not cached."""
    cache_file = cache_dir / f"{ykj_n}_{ykj_e}.json"
    
    if cache_file.exists():
        print(f"Data already cached for {ykj_n}:{ykj_e}")
        with open(cache_file, "r") as f:
            return json.load(f)
    
    print(f"Fetching data for {ykj_n}:{ykj_e}")
    data = fetch_square_data(ykj_n, ykj_e)
    with open(cache_file, "w") as f:
        json.dump(data, f)
    
    time.sleep(0.5)
    return data 