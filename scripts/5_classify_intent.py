"""
Step 5: Classify keyword intent using Groq API (LLaMA).
Reads raw/enriched.csv, classifies in batches of 500, saves output/keywords_final.csv.
Optionally sends Telegram notification.
"""

import csv
import json
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

INTENT_PROMPT = """Classify each keyword's search intent. Return ONLY a JSON array of objects.
Each object: {"keyword": "...", "intent": "..."}

Intent types:
- informational: user wants to learn something
- commercial: user is researching before buying
- transactional: user wants to buy/sign up/download
- navigational: user wants a specific website or page

Keywords:
{keywords}

Return ONLY valid JSON, no other text."""


def classify_batch(keywords: list[str]) -> dict:
    """Classify a batch of keywords via Groq API."""
    url = "https://api.groq.com/openai/v1/chat/completions"

    keywords_text = "\n".join(f"- {kw}" for kw in keywords)

    payload = {
        "model": config.GROQ_MODEL,
        "messages": [
            {"role": "user", "content": INTENT_PROMPT.format(keywords=keywords_text)},
        ],
        "temperature": 0,
        "max_tokens": 8000,
    }

    resp = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {config.GROQ_API_KEY}"},
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()

    content = data["choices"][0]["message"]["content"].strip()

    # Parse JSON from response (handle markdown code blocks)
    if content.startswith("```"):
        content = content.split("\n", 1)[1].rsplit("```", 1)[0]

    result_map = {}
    try:
        items = json.loads(content)
        for item in items:
            kw = item.get("keyword", "").lower()
            intent = item.get("intent", "informational").lower()
            if intent not in ("informational", "commercial", "transactional", "navigational"):
                intent = "informational"
            result_map[kw] = intent
    except json.JSONDecodeError:
        print(f"  WARNING: Failed to parse Groq response")

    return result_map


def send_telegram(message: str):
    """Send notification to Telegram."""
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(
        url,
        json={"chat_id": config.TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
        timeout=30,
    )


def main():
    print("[Step 5] Classifying intent...")

    input_path = os.path.join(config.RAW_DIR, "enriched.csv")
    with open(input_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"  Loaded {len(rows)} keywords")

    keywords_list = [row["keyword"] for row in rows]
    batch_size = config.GROQ_BATCH_SIZE

    intent_data = {}
    for i in range(0, len(keywords_list), batch_size):
        batch = keywords_list[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(keywords_list) - 1) // batch_size + 1
        print(f"  Intent batch {batch_num}/{total_batches} ({len(batch)} keywords)")

        try:
            result = classify_batch(batch)
            intent_data.update(result)
        except Exception as e:
            print(f"  ERROR in batch {batch_num}: {e}")
            # Assign default intent for failed batch
            for kw in batch:
                intent_data.setdefault(kw.lower(), "informational")

        time.sleep(2)  # Groq rate limit

    # Merge intent into rows
    for row in rows:
        kw = row["keyword"].lower()
        row["intent"] = intent_data.get(kw, "informational")

    # Save final output
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

    # Telegram notification
    msg = (
        f"<b>Keyword Pipeline Complete</b>\n\n"
        f"Total keywords: {len(rows)}\n"
        f"Intent breakdown:\n"
        + "\n".join(f"  {k}: {v}" for k, v in sorted(intent_counts.items()))
    )
    send_telegram(msg)
    print("  Done!")


if __name__ == "__main__":
    main()
