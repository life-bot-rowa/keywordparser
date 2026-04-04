"""
Step 3: Merge expanded.csv + competitor_keywords.csv.
Deduplicate, filter by volume, language heuristics, and stop words.
Saves to raw/merged.csv.
"""

import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def is_english(text: str) -> bool:
    """Simple heuristic: reject if >30% non-ASCII characters."""
    if not text:
        return False
    non_ascii = sum(1 for c in text if ord(c) > 127)
    return (non_ascii / len(text)) < 0.3


def contains_stop_word(keyword: str) -> bool:
    """Check if keyword contains any stop word."""
    kw_lower = keyword.lower()
    return any(sw in kw_lower for sw in config.STOP_WORDS)


def load_csv(path: str) -> list[dict]:
    """Load a CSV file, return empty list if not found."""
    if not os.path.exists(path):
        print(f"  WARNING: {path} not found, skipping")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    print("[Step 3] Merging and deduplicating...")

    expanded = load_csv(os.path.join(config.RAW_DIR, "expanded.csv"))
    competitor = load_csv(os.path.join(config.RAW_DIR, "competitor_keywords.csv"))

    print(f"  Expanded: {len(expanded)} rows")
    print(f"  Competitor: {len(competitor)} rows")

    # Deduplicate by keyword, keeping the one with highest volume
    seen = {}
    for row in expanded + competitor:
        kw = row.get("keyword", "").strip().lower()
        if not kw:
            continue

        volume = int(float(row.get("volume", 0) or 0))
        source = row.get("source", "")

        if kw not in seen or volume > int(float(seen[kw].get("volume", 0) or 0)):
            seen[kw] = {
                "keyword": kw,
                "volume": volume,
                "cpc": float(row.get("cpc", 0) or 0),
                "competition": float(row.get("competition", 0) or 0),
                "source": source,
            }

    print(f"  After dedup: {len(seen)} unique keywords")

    # Filter
    filtered = []
    removed_volume = 0
    removed_lang = 0
    removed_stop = 0

    for kw, data in seen.items():
        # if data["volume"] < config.MIN_VOLUME:
        #     removed_volume += 1
        #     continue
        if not is_english(kw):
            removed_lang += 1
            continue
        if contains_stop_word(kw):
            removed_stop += 1
            continue
        filtered.append(data)

    print(f"  Removed: {removed_volume} (low volume), {removed_lang} (non-english), {removed_stop} (stop words)")
    print(f"  Remaining: {len(filtered)} keywords")

    # Sort by volume descending
    filtered.sort(key=lambda x: x["volume"], reverse=True)

    output_path = os.path.join(config.RAW_DIR, "merged.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["keyword", "volume", "cpc", "competition", "source"]
        )
        writer.writeheader()
        writer.writerows(filtered)

    print(f"  Saved to {output_path}")


if __name__ == "__main__":
    main()
