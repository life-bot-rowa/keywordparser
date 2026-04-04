"""
Step 6: Merge competitor keywords with existing keywords_final.csv.
Reads raw/competitor_keywords.csv, merges with output/keywords_final.csv,
deduplicates, filters, classifies intent, saves back.
"""

import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# --- Filters (same as 3_merge_dedupe.py) ---

def is_english(text: str) -> bool:
    if not text:
        return False
    non_ascii = sum(1 for c in text if ord(c) > 127)
    return (non_ascii / len(text)) < 0.3


def contains_stop_word(keyword: str) -> bool:
    kw_lower = keyword.lower()
    return any(sw in kw_lower for sw in config.STOP_WORDS)


COMPETITION_MAP = {"LOW": 0.25, "MEDIUM": 0.5, "HIGH": 0.75, "UNSPECIFIED": 0}


def _parse_competition(val) -> float:
    if isinstance(val, str) and val in COMPETITION_MAP:
        return COMPETITION_MAP[val]
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


# --- Intent classification (same as 5_classify_intent.py) ---

TRANSACTIONAL_PATTERNS = [
    r"\b(buy|purchase|order|subscribe|sign up|signup|register|download|install)\b",
    r"\b(coupon|discount|deal|promo|voucher|cheap|affordable|price|pricing|cost)\b",
    r"\b(free trial|get started|start now|join|enroll)\b",
    r"\b(for sale|shop|store|checkout|cart)\b",
]
COMMERCIAL_PATTERNS = [
    r"\b(best|top|review|reviews|comparison|compare|vs|versus|alternative|alternatives)\b",
    r"\b(recommended|rating|ratings|ranked|ranking)\b",
    r"\b(worth it|pros and cons|should i)\b",
    r"\b(cheapest|fastest|most popular|highest rated)\b",
]
NAVIGATIONAL_PATTERNS = [
    r"\b(login|log in|sign in|signin|account|my account|dashboard)\b",
    r"\b(official|website|site|app|homepage)\b",
    r"\.(com|org|net|io|tv|app)\b",
]

TRANSACTIONAL_RE = [re.compile(p, re.IGNORECASE) for p in TRANSACTIONAL_PATTERNS]
COMMERCIAL_RE = [re.compile(p, re.IGNORECASE) for p in COMMERCIAL_PATTERNS]
NAVIGATIONAL_RE = [re.compile(p, re.IGNORECASE) for p in NAVIGATIONAL_PATTERNS]


def classify_intent(keyword: str) -> str:
    for pattern in TRANSACTIONAL_RE:
        if pattern.search(keyword):
            return "transactional"
    for pattern in COMMERCIAL_RE:
        if pattern.search(keyword):
            return "commercial"
    for pattern in NAVIGATIONAL_RE:
        if pattern.search(keyword):
            return "navigational"
    return "informational"


def main():
    print("[Step 6] Merging competitor keywords with existing data...")

    # Load existing keywords_final.csv
    existing = {}
    if os.path.exists(config.OUTPUT_FILE):
        with open(config.OUTPUT_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                kw = row.get("keyword", "").strip().lower()
                if kw:
                    existing[kw] = row
        print(f"  Existing keywords: {len(existing)}")

    # Load competitor keywords
    competitor_path = os.path.join(config.RAW_DIR, "competitor_keywords.csv")
    if not os.path.exists(competitor_path):
        print(f"  ERROR: {competitor_path} not found")
        sys.exit(1)

    with open(competitor_path, "r", encoding="utf-8") as f:
        competitor_rows = list(csv.DictReader(f))
    print(f"  Competitor keywords: {len(competitor_rows)}")

    # Merge: add new competitor keywords
    added = 0
    skipped_volume = 0
    skipped_lang = 0
    skipped_stop = 0

    for row in competitor_rows:
        kw = row.get("keyword", "").strip().lower()
        if not kw:
            continue

        # Skip if already exists
        if kw in existing:
            continue

        volume = int(float(row.get("volume", 0) or 0))
        if volume < config.MIN_VOLUME:
            skipped_volume += 1
            continue
        if not is_english(kw):
            skipped_lang += 1
            continue
        if contains_stop_word(kw):
            skipped_stop += 1
            continue

        existing[kw] = {
            "keyword": kw,
            "volume": volume,
            "cpc": float(row.get("cpc", 0) or 0),
            "competition": _parse_competition(row.get("competition", 0)),
            "keyword_difficulty": row.get("keyword_difficulty", 0),
            "intent": classify_intent(kw),
            "source": "competitor",
        }
        added += 1

    print(f"  Added: {added} new keywords")
    print(f"  Skipped: {skipped_volume} (low volume), {skipped_lang} (non-english), {skipped_stop} (stop words)")

    # Sort by volume
    rows = list(existing.values())
    rows.sort(key=lambda x: int(float(x.get("volume", 0) or 0)), reverse=True)

    # Save
    os.makedirs(os.path.dirname(config.OUTPUT_FILE), exist_ok=True)
    with open(config.OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "keyword", "volume", "cpc", "competition",
                "keyword_difficulty", "intent", "source",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "keyword": row.get("keyword", ""),
                "volume": row.get("volume", 0),
                "cpc": row.get("cpc", 0),
                "competition": row.get("competition", 0),
                "keyword_difficulty": row.get("keyword_difficulty", 0),
                "intent": row.get("intent", "informational"),
                "source": row.get("source", ""),
            })

    print(f"  Total: {len(rows)} keywords")
    print(f"  Saved to {config.OUTPUT_FILE}")
    print("  Done!")


if __name__ == "__main__":
    main()
