#!/usr/bin/env python3
"""
Tag frequency histogram and statistics display

Implements constraints: OPT-012, OPT-013, OPT-014, OPT-018, OPT-018a, OPT-018b, OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.10.yaml
"""

import sys
import json
import sqlite3
from pathlib import Path
from collections import Counter

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml

# INV-021: Absolute paths only - read from config
# First, determine config path relative to this script
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
CONFIG_PATH = BASE_DIR / "config" / "deployment.yaml"

# Load config to get project root and context engine home
with open(CONFIG_PATH) as f:
    _config = yaml.safe_load(f)
    PROJECT_ROOT = Path(_config['paths']['project_root'])
    # Read context_engine_home from config - allows .context-engine to be placed anywhere
    BASE_DIR = Path(_config['paths']['context_engine_home'])


def load_config():
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def get_tag_frequency(db_path):
    """OPT-012: Track tag reuse frequency across database."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get all rules with tags
    cursor.execute("""
        SELECT tags
        FROM rules
        WHERE tags IS NOT NULL AND tags != '[]'
    """)

    tag_counter = Counter()
    for row in cursor.fetchall():
        tags_json = row[0]
        try:
            tags = json.loads(tags_json)
            if isinstance(tags, list):
                tag_counter.update(tags)
        except json.JSONDecodeError:
            continue

    conn.close()
    return tag_counter


def print_histogram(tag_counter, top_n=30):
    """OPT-018, OPT-018a: Display visual bar chart of tag frequency distribution."""
    if not tag_counter:
        return

    # Get top N most frequent tags
    most_common = tag_counter.most_common(top_n)

    # Find max frequency for scaling bars
    max_freq = most_common[0][1] if most_common else 1
    max_bar_width = 50

    print("\nTag Frequency Distribution (Top 30)")
    print("="*70)

    for tag, count in most_common:
        # Calculate proportional bar width
        bar_width = int((count / max_freq) * max_bar_width)
        bar = '█' * bar_width
        print(f"  {tag:30} {bar} {count}")


def identify_low_frequency_tags(tag_counter, threshold=2):
    """OPT-014: Identify low-frequency tags for review."""
    low_freq_tags = [(tag, count) for tag, count in tag_counter.items() if count <= threshold]
    return sorted(low_freq_tags, key=lambda x: x[1])


def main():
    """Display tag frequency histogram and statistics."""
    # Load configuration
    config = load_config()
    db_path = BASE_DIR / config['paths']['database']

    # OPT-074: Check for empty database state
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_count = cursor.fetchone()[0]

    if tagged_count == 0:
        # Empty state reporting
        cursor.execute("SELECT COUNT(*) FROM rules")
        total_rules = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
        needs_tags_count = cursor.fetchone()[0]

        conn.close()

        print("Tag Usage Statistics")
        print("="*70)
        print("")
        print("No tagged rules in database.")
        print("")
        print(f"Database contains {total_rules} rules, {needs_tags_count} awaiting tag optimization.")
        print("")

        if total_rules == 0:
            print("Run 'make chatlogs-extract' to import rules from chatlogs first.")
        elif needs_tags_count > 0:
            print("Run 'make tags-optimize' or 'make tags-optimize-auto' to begin tagging.")
        else:
            print("All rules have empty tag lists. Check database integrity.")

        return 0

    conn.close()

    # OPT-012: Get tag frequency
    tag_counter = get_tag_frequency(db_path)

    print("Tag Usage Statistics")
    print("="*70)

    # OPT-018b: Summary statistics
    total_unique_tags = len(tag_counter)
    total_tag_instances = sum(tag_counter.values())

    print(f"\nSummary:")
    print(f"  Total unique tags: {total_unique_tags}")
    print(f"  Total tag instances: {total_tag_instances}")
    print(f"  Average tags per rule: {total_tag_instances / tagged_count:.1f}")

    # OPT-018, OPT-018a: Display histogram
    print_histogram(tag_counter, top_n=30)

    # OPT-014: Identify low-frequency tags
    low_freq_tags = identify_low_frequency_tags(tag_counter, threshold=2)
    if low_freq_tags:
        print(f"\nLow-Frequency Tags (used ≤2 times): {len(low_freq_tags)} tags")
        print("="*70)
        for tag, count in low_freq_tags[:20]:
            print(f"  {tag:30} {count}")
        if len(low_freq_tags) > 20:
            print(f"  ... and {len(low_freq_tags) - 20} more")

    return 0


if __name__ == '__main__':
    sys.exit(main())
