"""
Step 1: Expand seed keywords via DataForSEO Keywords For Keywords API.
Reads seeds.txt, fetches ~1000 related keywords per seed, saves to raw/expanded.csv.
"""

import csv
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def get_related_keywords(seeds: list[str]) -> list[dict]:
    """Fetch related keywords from DataForSEO Keywords For Keywords API."""
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/related_keywords/live"

    all_keywords = []

    for seed in seeds:
        print(f"  Expanding seed: '{seed}'...")
        payload = [
            {
                "keyword": seed,
                "language_code": config.LANGUAGE,
                "location_code": config.LOCATION_CODE,
                "include_seed_keyword": True,
                "depth": 2,
                "limit": 1000,
            }
        ]

        resp = requests.post(
            url,
            json=payload,
            auth=(config.DATAFORSEO_LOGIN, config.DATAFORSEO_PASSWORD),
            timeout=120,
        )
        if resp.status_code == 402:
            print(f"  WARNING: 402 Payment Required — skipping seed '{seed}'")
            continue
        resp.raise_for_status()
        data = resp.json()

        if data.get("status_code") != 20000:
            print(f"  WARNING: API error for '{seed}': {data.get('status_message')}")
            continue

        for task in data.get("tasks", []):
            result = task.get("result")
            if not result:
                continue
            for item in result:
                for kw_item in item.get("items", []):
                    kw_data = kw_item.get("keyword_data", {})
                    kw_info = kw_data.get("keyword_info", {})
                    all_keywords.append(
                        {
                            "keyword": kw_data.get("keyword", ""),
                            "volume": kw_info.get("search_volume", 0),
                            "cpc": kw_info.get("cpc", 0),
                            "competition": kw_info.get("competition", 0),
                            "source": "seed_expansion",
                            "seed": seed,
                        }
                    )

        time.sleep(1)  # rate limit

    return all_keywords


def main():
    print("[Step 1] Expanding seeds...")

    with open(config.SEEDS_FILE, "r") as f:
        seeds = [line.strip() for line in f if line.strip()]

    print(f"  Found {len(seeds)} seeds")
    keywords = get_related_keywords(seeds)
    print(f"  Got {len(keywords)} keywords total")

    os.makedirs(config.RAW_DIR, exist_ok=True)
    output_path = os.path.join(config.RAW_DIR, "expanded.csv")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["keyword", "volume", "cpc", "competition", "source", "seed"]
        )
        writer.writeheader()
        writer.writerows(keywords)

    print(f"  Saved to {output_path}")


if __name__ == "__main__":
    main()
