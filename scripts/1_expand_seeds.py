"""
Step 1: Expand seed keywords via 3 sources per seed:
  1) Google Ads API — GenerateKeywordIdeas (free with basic access)
  2) DataForSEO — Keyword Suggestions (unique SERP-based algorithm)
  3) DataForSEO — Related Keywords (depth-based expansion)
Reads seeds.txt, saves all results to raw/expanded.csv.
"""

import csv
import os
import sys
import time

import requests
from google.ads.googleads.client import GoogleAdsClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

AUTH = (config.DATAFORSEO_LOGIN, config.DATAFORSEO_PASSWORD)
BASE = "https://api.dataforseo.com/v3/dataforseo_labs/google"


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


def google_keyword_ideas(seed: str) -> list[dict]:
    """Fetch keyword ideas from Google Ads Keyword Planner."""
    client = get_google_ads_client()
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
    request = client.get_type("GenerateKeywordIdeasRequest")

    request.customer_id = config.GOOGLE_ADS_CUSTOMER_ID
    request.language = client.get_service("GoogleAdsService").language_constant_path("1000")  # English
    request.geo_target_constants.append(
        client.get_service("GoogleAdsService").geo_target_constant_path("2840")  # US
    )
    request.keyword_seed.keywords.append(seed)
    request.include_adult_keywords = False

    keywords = []
    try:
        response = keyword_plan_idea_service.generate_keyword_ideas(request=request)
        for idea in response:
            metrics = idea.keyword_idea_metrics
            keywords.append({
                "keyword": idea.text,
                "volume": metrics.avg_monthly_searches or 0,
                "cpc": (metrics.average_cpc_micros or 0) / 1_000_000,
                "competition": metrics.competition.name if metrics.competition else "UNSPECIFIED",
            })
    except Exception as e:
        print(f"    ERROR: Google Ads API: {e}", flush=True)

    return keywords


# --- DataForSEO helpers ---

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


def parse_keyword_suggestions(data: dict) -> list[dict]:
    """Parse response from DataForSEO keyword_suggestions endpoint."""
    keywords = []
    for task in data.get("tasks", []):
        for res in task.get("result", []) or []:
            items = res.get("items") or []
            for item in items:
                kw_info = item.get("keyword_info", {})
                keywords.append({
                    "keyword": item.get("keyword", ""),
                    "volume": kw_info.get("search_volume", 0),
                    "cpc": kw_info.get("cpc", 0),
                    "competition": kw_info.get("competition", 0),
                })
    return keywords


def parse_related_keywords(data: dict) -> list[dict]:
    """Parse response from DataForSEO related_keywords endpoint."""
    keywords = []
    for task in data.get("tasks", []):
        for res in task.get("result", []) or []:
            items = res.get("items") or []
            for item in items:
                kw_data = item.get("keyword_data", {})
                kw_info = kw_data.get("keyword_info", {})
                keywords.append({
                    "keyword": kw_data.get("keyword", ""),
                    "volume": kw_info.get("search_volume", 0),
                    "cpc": kw_info.get("cpc", 0),
                    "competition": kw_info.get("competition", 0),
                })
    return keywords


def paginated_fetch(endpoint: str, base_payload: dict, parser, label: str) -> list[dict]:
    """Fetch all pages from a DataForSEO endpoint using offset pagination."""
    all_kw = []
    offset = 0
    limit = 1000
    page = 1

    while True:
        payload = {**base_payload, "limit": limit, "offset": offset}
        print(f"    page {page} (offset {offset})", flush=True)
        data = api_post(endpoint, [payload])
        if not data:
            break

        kws = parser(data)
        print(f"    -> {len(kws)} keywords", flush=True)
        if not kws:
            break

        all_kw.extend(kws)

        if len(kws) < limit:
            break  # last page

        offset += limit
        page += 1
        time.sleep(1)

    return all_kw


def expand_seed(seed: str) -> list[dict]:
    """Run all 3 sources for a single seed."""
    all_kw = []

    # 1) Google Ads API — Keyword Ideas (free)
    print(f"  [Google Ads] keyword_ideas '{seed}'", flush=True)
    kws = google_keyword_ideas(seed)
    print(f"  Google Ads total: {len(kws)}", flush=True)
    all_kw.extend(kws)
    time.sleep(1)

    # 2) DataForSEO — Keyword Suggestions (unique algorithm)
    print(f"  [DataForSEO] keyword_suggestions '{seed}'", flush=True)
    kws = paginated_fetch(
        f"{BASE}/keyword_suggestions/live",
        {"keyword": seed, "language_code": config.LANGUAGE, "location_code": config.LOCATION_CODE},
        parse_keyword_suggestions,
        "keyword_suggestions",
    )
    print(f"  keyword_suggestions total: {len(kws)}", flush=True)
    all_kw.extend(kws)
    time.sleep(1)

    # 3) DataForSEO — Related Keywords (depth-based)
    print(f"  [DataForSEO] related_keywords '{seed}'", flush=True)
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
    print("[Step 1] Expanding seeds (Google Ads + DataForSEO)...")

    with open(config.SEEDS_FILE, "r") as f:
        seeds = [line.strip() for line in f if line.strip()]

    print(f"  Found {len(seeds)} seeds")

    all_keywords = []
    for seed in seeds:
        print(f"\n  === Seed: '{seed}' ===", flush=True)
        try:
            kws = expand_seed(seed)
            for kw in kws:
                kw["source"] = "seed_expansion"
                kw["seed"] = seed
            all_keywords.extend(kws)
        except Exception as e:
            print(f"  ERROR processing seed '{seed}': {e}", flush=True)
            print(f"  Continuing with next seed...", flush=True)

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
