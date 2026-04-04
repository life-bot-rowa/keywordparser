"""Run the full keyword pipeline sequentially."""

import subprocess
import sys

SCRIPTS = [
    "scripts/1_expand_seeds.py",
    "scripts/2_competitor_keywords.py",
    "scripts/3_merge_dedupe.py",
    "scripts/4_enrich_data.py",
    "scripts/5_classify_intent.py",
]


def main():
    for script in SCRIPTS:
        print(f"\n{'='*60}")
        print(f"Running {script}...")
        print('='*60)
        result = subprocess.run([sys.executable, script])
        if result.returncode != 0:
            print(f"FAILED: {script} (exit code {result.returncode})")
            sys.exit(1)

    print(f"\n{'='*60}")
    print("Pipeline complete!")
    print('='*60)


if __name__ == "__main__":
    main()
