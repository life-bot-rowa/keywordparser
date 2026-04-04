"""
Step 7: Enrich competitor keywords with Keyword Difficulty from DataForSEO.
Reads raw/competitor_keywords.csv, adds KD, saves back.
"""

import csv
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def enrich_keyword_difficulty(keywords: list[str]) -> dict:
    """Fetch keyword difficulty from DataForSEO."""
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
    if resp.status_code == 402:
        print("  WARNING: 402 Payment Required — skipping KD batch", flush=True)
        return {}
    resp.raise_for_status()
    data = resp.json()

    result_map = {}
    if data.get("status_code") == 20000:
        for task in data.get("tasks", []):
            for res in task.get("result", []) or []:
                items = res.get("items") or []
                for item in items:
                    kw = item.get("keyword", "").lower()
                    result_map[kw] = item.get("keyword_difficulty", 0)

    return result_map


def main():
    print("[Step 7] Enriching competitor keywords with KD...")

    input_path = os.path.join(config.RAW_DIR, "competitor_keywords.csv")
    if not os.path.exists(input_path):
        print(f"  ERROR: {input_path} not found")
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"  Loaded {len(rows)} keywords")

    keywords_list = [row["keyword"] for row in rows]
    batch_size = config.DATAFORSEO_BATCH_SIZE

    kd_data = {}
    for i in range(0, len(keywords_list), batch_size):
        batch = keywords_list[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(keywords_list) - 1) // batch_size + 1
        print(f"  KD batch {batch_num}/{total_batches} ({len(batch)} keywords)", flush=True)
        try:
            result = enrich_keyword_difficulty(batch)
            kd_data.update(result)
        except Exception as e:
            print(f"  ERROR in batch {batch_num}: {e}", flush=True)
        time.sleep(1)

    # Update rows with KD
    updated = 0
    for row in rows:
        kw = row["keyword"].lower()
        kd = kd_data.get(kw)
        if kd is not None:
            row["keyword_difficulty"] = kd
            updated += 1

    print(f"  Updated KD for {updated}/{len(rows)} keywords")

    # Save back
    fieldnames = list(rows[0].keys()) if rows else []
    if "keyword_difficulty" not in fieldnames:
        fieldnames.append("keyword_difficulty")

    with open(input_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Saved to {input_path}")
    print("  Done!")


if __name__ == "__main__":
    main()
