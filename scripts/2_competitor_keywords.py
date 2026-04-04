"""
Step 2: Get keywords that competitors rank for via DataForSEO Domain Keywords API.
Reads competitors.txt, fetches all ranking keywords, saves to raw/competitor_keywords.csv.
"""

import csv
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def get_domain_keywords(domain: str) -> list[dict]:
    """Fetch keywords a domain ranks for via DataForSEO Ranked Keywords API."""
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/ranked_keywords/live"

    max_keywords = config.MAX_KEYWORDS_PER_DOMAIN
    all_keywords = []
    offset = 0
    limit = 1000

    while True:
        payload = [
            {
                "target": domain,
                "language_code": config.LANGUAGE,
                "location_code": config.LOCATION_CODE,
                "limit": limit,
                "offset": offset,
                "order_by": ["keyword_data.keyword_info.search_volume,desc"],
            }
        ]

        resp = requests.post(
            url,
            json=payload,
            auth=(config.DATAFORSEO_LOGIN, config.DATAFORSEO_PASSWORD),
            timeout=120,
        )
        if resp.status_code == 402:
            print(f"  WARNING: 402 Payment Required — stopping pagination for '{domain}' (got {len(all_keywords)} keywords)")
            break
        resp.raise_for_status()
        data = resp.json()

        if data.get("status_code") != 20000:
            print(f"  WARNING: API error for '{domain}': {data.get('status_message')}")
            break

        items_found = False
        for task in data.get("tasks", []):
            result = task.get("result")
            if not result:
                continue
            for res_item in result:
                items = res_item.get("items", [])
                if not items:
                    continue
                items_found = True
                for item in items:
                    kw_data = item.get("keyword_data", {})
                    kw_info = kw_data.get("keyword_info", {})
                    ranked = item.get("ranked_serp_element", {})
                    all_keywords.append(
                        {
                            "keyword": kw_data.get("keyword", ""),
                            "volume": kw_info.get("search_volume", 0),
                            "cpc": kw_info.get("cpc", 0),
                            "competition": kw_info.get("competition", 0),
                            "position": ranked.get("serp_item", {}).get("rank_absolute", 0),
                            "source": "competitor",
                            "competitor": domain,
                        }
                    )

        if not items_found:
            break

        if len(all_keywords) >= max_keywords:
            print(f"    Reached limit of {max_keywords} keywords for '{domain}'")
            all_keywords = all_keywords[:max_keywords]
            break

        offset += limit
        print(f"    Fetched {len(all_keywords)} keywords so far...")
        time.sleep(1)

    return all_keywords


def main():
    print("[Step 2] Fetching competitor keywords...")

    with open(config.COMPETITORS_FILE, "r") as f:
        competitors = [line.strip() for line in f if line.strip()]

    print(f"  Found {len(competitors)} competitors")

    all_keywords = []
    for domain in competitors:
        print(f"  Processing: {domain}")
        keywords = get_domain_keywords(domain)
        print(f"    Got {len(keywords)} keywords")
        all_keywords.extend(keywords)
        time.sleep(2)

    print(f"  Total: {len(all_keywords)} keywords")

    os.makedirs(config.RAW_DIR, exist_ok=True)
    output_path = os.path.join(config.RAW_DIR, "competitor_keywords.csv")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "keyword", "volume", "cpc", "competition",
                "position", "source", "competitor",
            ],
        )
        writer.writeheader()
        writer.writerows(all_keywords)

    print(f"  Saved to {output_path}")


if __name__ == "__main__":
    main()
