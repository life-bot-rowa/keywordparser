"""
Step 4: Enrich keywords with volume, CPC, competition, and keyword difficulty.
Reads raw/merged.csv, queries DataForSEO in batches, updates data.
Saves to raw/enriched.csv.
"""

import csv
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def enrich_search_volume(keywords: list[str]) -> dict:
    """Fetch search volume data for a batch of keywords."""
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"

    payload = [
        {
            "keywords": keywords,
            "language_code": config.LANGUAGE,
            "location_code": config.LOCATION_CODE,
        }
    ]

    resp = requests.post(
        url,
        json=payload,
        auth=(config.DATAFORSEO_LOGIN, config.DATAFORSEO_PASSWORD),
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()

    result_map = {}
    if data.get("status_code") == 20000:
        for task in data.get("tasks", []):
            for res in task.get("result", []) or []:
                kw = res.get("keyword", "").lower()
                result_map[kw] = {
                    "volume": res.get("search_volume", 0),
                    "cpc": res.get("cpc", 0),
                    "competition": res.get("competition", 0),
                    "monthly_searches": res.get("monthly_searches", []),
                }

    return result_map


def enrich_keyword_difficulty(keywords: list[str]) -> dict:
    """Fetch keyword difficulty for a batch of keywords."""
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/bulk_keyword_difficulty/live"

    payload = [
        {
            "keywords": keywords,
            "language_code": config.LANGUAGE,
            "location_code": config.LOCATION_CODE,
        }
    ]

    resp = requests.post(
        url,
        json=payload,
        auth=(config.DATAFORSEO_LOGIN, config.DATAFORSEO_PASSWORD),
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()

    result_map = {}
    if data.get("status_code") == 20000:
        for task in data.get("tasks", []):
            for res in task.get("result", []) or []:
                items = res.get("items", [])
                if items:
                    for item in items:
                        kw = item.get("keyword", "").lower()
                        result_map[kw] = {
                            "keyword_difficulty": item.get("keyword_difficulty", 0),
                        }

    return result_map


def main():
    print("[Step 4] Enriching keyword data...")

    input_path = os.path.join(config.RAW_DIR, "merged.csv")
    with open(input_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"  Loaded {len(rows)} keywords")

    keywords_list = [row["keyword"] for row in rows]
    batch_size = config.DATAFORSEO_BATCH_SIZE

    # Enrich search volume in batches
    volume_data = {}
    for i in range(0, len(keywords_list), batch_size):
        batch = keywords_list[i : i + batch_size]
        print(f"  Search volume batch {i // batch_size + 1}/{(len(keywords_list) - 1) // batch_size + 1} ({len(batch)} keywords)")
        result = enrich_search_volume(batch)
        volume_data.update(result)
        time.sleep(1)

    # Enrich keyword difficulty in batches
    kd_data = {}
    for i in range(0, len(keywords_list), batch_size):
        batch = keywords_list[i : i + batch_size]
        print(f"  KD batch {i // batch_size + 1}/{(len(keywords_list) - 1) // batch_size + 1} ({len(batch)} keywords)")
        result = enrich_keyword_difficulty(batch)
        kd_data.update(result)
        time.sleep(1)

    # Merge data
    for row in rows:
        kw = row["keyword"].lower()
        vol = volume_data.get(kw, {})
        kd = kd_data.get(kw, {})

        if vol.get("volume"):
            row["volume"] = vol["volume"]
        if vol.get("cpc"):
            row["cpc"] = vol["cpc"]
        if vol.get("competition"):
            row["competition"] = vol["competition"]
        row["keyword_difficulty"] = kd.get("keyword_difficulty", 0)

    output_path = os.path.join(config.RAW_DIR, "enriched.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "keyword", "volume", "cpc", "competition",
                "keyword_difficulty", "source",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "keyword": row["keyword"],
                    "volume": row.get("volume", 0),
                    "cpc": row.get("cpc", 0),
                    "competition": row.get("competition", 0),
                    "keyword_difficulty": row.get("keyword_difficulty", 0),
                    "source": row.get("source", ""),
                }
            )

    print(f"  Saved to {output_path}")


if __name__ == "__main__":
    main()
