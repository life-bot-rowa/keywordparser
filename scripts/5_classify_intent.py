"""
Step 5: Classify keyword intent using rule-based approach.
Reads raw/enriched.csv (or merged.csv, or output/keywords_final.csv),
classifies intent, saves output/keywords_final.csv.
"""

import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# --- Intent classification rules ---

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
    r"\b(netflix|hulu|disney|amazon prime|hbo|tubi|peacock|paramount|apple tv)\b",
    r"\b(youtube|imdb|rotten tomatoes|letterboxd|justwatch)\b",
]

TRANSACTIONAL_RE = [re.compile(p, re.IGNORECASE) for p in TRANSACTIONAL_PATTERNS]
COMMERCIAL_RE = [re.compile(p, re.IGNORECASE) for p in COMMERCIAL_PATTERNS]
NAVIGATIONAL_RE = [re.compile(p, re.IGNORECASE) for p in NAVIGATIONAL_PATTERNS]


def classify_intent(keyword: str) -> str:
    """Classify a single keyword's search intent."""
    # Check transactional first (strongest signal)
    for pattern in TRANSACTIONAL_RE:
        if pattern.search(keyword):
            return "transactional"

    # Check commercial
    for pattern in COMMERCIAL_RE:
        if pattern.search(keyword):
            return "commercial"

    # Check navigational
    for pattern in NAVIGATIONAL_RE:
        if pattern.search(keyword):
            return "navigational"

    # Default
    return "informational"


def main():
    print("[Step 5] Classifying intent (rule-based)...")

    # Try multiple input sources
    enriched = os.path.join(config.RAW_DIR, "enriched.csv")
    merged = os.path.join(config.RAW_DIR, "merged.csv")
    final = config.OUTPUT_FILE

    if os.path.exists(enriched):
        input_path = enriched
    elif os.path.exists(merged):
        input_path = merged
    elif os.path.exists(final):
        input_path = final
    else:
        print("  ERROR: No input file found")
        sys.exit(1)

    print(f"  Reading from: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"  Loaded {len(rows)} keywords")

    # Classify
    for row in rows:
        row["intent"] = classify_intent(row["keyword"])

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
            writer.writerow(
                {
                    "keyword": row["keyword"],
                    "volume": row.get("volume", 0),
                    "cpc": row.get("cpc", 0),
                    "competition": row.get("competition", 0),
                    "keyword_difficulty": row.get("keyword_difficulty", 0),
                    "intent": row.get("intent", "informational"),
                    "source": row.get("source", ""),
                }
            )

    print(f"  Saved {len(rows)} keywords to {config.OUTPUT_FILE}")

    # Stats
    intent_counts = {}
    for row in rows:
        intent = row.get("intent", "informational")
        intent_counts[intent] = intent_counts.get(intent, 0) + 1

    stats = "\n".join(f"  {k}: {v}" for k, v in sorted(intent_counts.items()))
    print(f"  Intent distribution:\n{stats}")
    print("  Done!")


if __name__ == "__main__":
    main()
