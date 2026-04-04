"""
Step 1: Expand seed keywords via 3 DataForSEO endpoints per seed:
  1) Keyword Ideas (keyword_ideas)
  2) Keyword Suggestions (keyword_suggestions)
  3) Related Keywords (related_keywords)
Reads seeds.txt, saves all results to raw/expanded.csv.
"""

import csv
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

AUTH = (config.DATAFORSEO_LOGIN, config.DATAFORSEO_PASSWORD)
BASE = "https://api.dataforseo.com/v3/dataforseo_labs/google"


def api_post(url: str, payload: list[dict]) -> dict | None:
    """Make a POST request to DataForSEO, return parsed JSON or None on error."""
    resp = requests.post(url, json=payload, auth=AUTH, timeout=120)
    print(f"    HTTP {resp.status_code}", flush=True)
    if resp.status_code == 402:
        print(f"    WARNING: 402 Payment Required", flush=True)
        return None
    if resp.status_code != 200:
        print(f"    WARNING: {resp.text[:300]}", flush=True)
        return None
    data = resp.json()
    if data.get("status_code") != 20000:
        print(f"    WARNING: API {data.get('status_code')} {data.get('status_message')}", flush=True)
        return None
    return data


def parse_keyword_ideas(data: dict) -> list[dict]:
    """Parse response from keyword_ideas endpoint."""
    keywords = []
    for task in data.get("tasks", []):
        for res in task.get("result", []) or []:
            for item in res.get("items", []):
                kw_info = item.get("keyword_info", {})
                keywords.append({
                    "keyword": item.get("keyword", ""),
                    "volume": kw_info.get("search_volume", 0),
                    "cpc": kw_info.get("cpc", 0),
                    "competition": kw_info.get("competition", 0),
                })
    return keywords


def parse_keyword_suggestions(data: dict) -> list[dict]:
    """Parse response from keyword_suggestions endpoint."""
    keywords = []
    for task in data.get("tasks", []):
        for res in task.get("result", []) or []:
            for item in res.get("items", []):
                kw_info = item.get("keyword_info", {})
                keywords.append({
                    "keyword": item.get("keyword", ""),
                    "volume": kw_info.get("search_volume", 0),
                    "cpc": kw_info.get("cpc", 0),
                    "competition": kw_info.get("competition", 0),
                })
    return keywords


def parse_related_keywords(data: dict) -> list[dict]:
    """Parse response from related_keywords endpoint."""
    keywords = []
    for task in data.get("tasks", []):
        for res in task.get("result", []) or []:
            for item in res.get("items", []):
                kw_data = item.get("keyword_data", {})
                kw_info = kw_data.get("keyword_info", {})
                keywords.append({
                    "keyword": kw_data.get("keyword", ""),
                    "volume": kw_info.get("search_volume", 0),
                    "cpc": kw_info.get("cpc", 0),
                    "competition": kw_info.get("competition", 0),
                })
    return keywords


def expand_seed(seed: str) -> list[dict]:
    """Run all 3 endpoints for a single seed."""
    all_kw = []

    # 1) Keyword Ideas (accepts array of keywords)
    print(f"  [keyword_ideas] '{seed}'", flush=True)
    data = api_post(f"{BASE}/keyword_ideas/live", [{
        "keywords": [seed],
        "language_code": config.LANGUAGE,
        "location_code": config.LOCATION_CODE,
        "limit": 1000,
    }])
    if data:
        kws = parse_keyword_ideas(data)
        print(f"    -> {len(kws)} keywords", flush=True)
        all_kw.extend(kws)
    time.sleep(1)

    # 2) Keyword Suggestions (accepts single keyword)
    print(f"  [keyword_suggestions] '{seed}'", flush=True)
    data = api_post(f"{BASE}/keyword_suggestions/live", [{
        "keyword": seed,
        "language_code": config.LANGUAGE,
        "location_code": config.LOCATION_CODE,
        "limit": 1000,
    }])
    if data:
        kws = parse_keyword_suggestions(data)
        print(f"    -> {len(kws)} keywords", flush=True)
        all_kw.extend(kws)
    time.sleep(1)

    # 3) Related Keywords (accepts single keyword)
    print(f"  [related_keywords] '{seed}'", flush=True)
    data = api_post(f"{BASE}/related_keywords/live", [{
        "keyword": seed,
        "language_code": config.LANGUAGE,
        "location_code": config.LOCATION_CODE,
        "include_seed_keyword": True,
        "depth": 4,
        "limit": 1000,
    }])
    if data:
        kws = parse_related_keywords(data)
        print(f"    -> {len(kws)} keywords", flush=True)
        all_kw.extend(kws)
    time.sleep(1)

    return all_kw


def main():
    print("[Step 1] Expanding seeds (3 endpoints per seed)...")

    with open(config.SEEDS_FILE, "r") as f:
        seeds = [line.strip() for line in f if line.strip()]

    print(f"  Found {len(seeds)} seeds")

    all_keywords = []
    for seed in seeds:
        print(f"\n  === Seed: '{seed}' ===", flush=True)
        kws = expand_seed(seed)
        for kw in kws:
            kw["source"] = "seed_expansion"
            kw["seed"] = seed
        all_keywords.extend(kws)

    print(f"\n  Total: {len(all_keywords)} keywords (before dedup)")

    os.makedirs(config.RAW_DIR, exist_ok=True)
    output_path = os.path.join(config.RAW_DIR, "expanded.csv")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["keyword", "volume", "cpc", "competition", "source", "seed"]
        )
        writer.writeheader()
        writer.writerows(all_keywords)

    print(f"  Saved to {output_path}")


if __name__ == "__main__":
    main()
