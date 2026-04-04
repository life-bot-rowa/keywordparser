"""
Step 4: Enrich keywords with volume, CPC, competition (Google Ads API)
and keyword difficulty (DataForSEO).
Reads raw/merged.csv, saves to raw/enriched.csv.
"""

import csv
import os
import sys
import time

import requests
from google.ads.googleads.client import GoogleAdsClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# --- Google Ads API ---

def get_google_ads_client() -> GoogleAdsClient:
    """Create Google Ads API client from config."""
    credentials = {
        "developer_token": config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": config.GOOGLE_ADS_CLIENT_ID,
        "client_secret": config.GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": config.GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(credentials)


def enrich_search_volume(keywords: list[str]) -> dict:
    """Fetch search volume data via Google Ads Keyword Planner."""
    client = get_google_ads_client()
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
    request = client.get_type("GenerateKeywordIdeasRequest")

    request.customer_id = config.GOOGLE_ADS_CUSTOMER_ID
    request.language = client.get_service("GoogleAdsService").language_constant_path("1000")
    request.geo_target_constants.append(
        client.get_service("GoogleAdsService").geo_target_constant_path("2840")
    )
    request.keyword_seed.keywords.extend(keywords)
    request.include_adult_keywords = False

    result_map = {}
    try:
        response = keyword_plan_idea_service.generate_keyword_ideas(request=request)
        for idea in response:
            metrics = idea.keyword_idea_metrics
            kw = idea.text.lower()
            result_map[kw] = {
                "volume": metrics.avg_monthly_searches or 0,
                "cpc": (metrics.average_cpc_micros or 0) / 1_000_000,
                "competition": metrics.competition.name if metrics.competition else "UNSPECIFIED",
            }
    except Exception as e:
        print(f"  ERROR: Google Ads API: {e}", flush=True)

    return result_map


# --- DataForSEO (KD only) ---

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
        print("  WARNING: 402 Payment Required — skipping KD batch")
        return {}
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
    print("[Step 4] Enriching keyword data (Google Ads + DataForSEO KD)...")

    input_path = os.path.join(config.RAW_DIR, "merged.csv")
    with open(input_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"  Loaded {len(rows)} keywords")

    keywords_list = [row["keyword"] for row in rows]

    # Google Ads: volume + CPC + competition (batches of 20 — API limit)
    volume_data = {}
    batch_size = 20
    for i in range(0, len(keywords_list), batch_size):
        batch = keywords_list[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(keywords_list) - 1) // batch_size + 1
        print(f"  [Google Ads] volume batch {batch_num}/{total_batches} ({len(batch)} keywords)", flush=True)
        result = enrich_search_volume(batch)
        volume_data.update(result)
        print(f"    -> got data for {len(result)} keywords", flush=True)
        time.sleep(1)

    # DataForSEO: keyword difficulty (batches of 1000)
    kd_data = {}
    kd_batch_size = config.DATAFORSEO_BATCH_SIZE
    for i in range(0, len(keywords_list), kd_batch_size):
        batch = keywords_list[i : i + kd_batch_size]
        batch_num = i // kd_batch_size + 1
        total_batches = (len(keywords_list) - 1) // kd_batch_size + 1
        print(f"  [DataForSEO] KD batch {batch_num}/{total_batches} ({len(batch)} keywords)", flush=True)
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
