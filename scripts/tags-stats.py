#!/usr/bin/env python3
"""
Tag frequency histogram and statistics display

Implements constraints: OPT-012, OPT-013, OPT-014, OPT-018, OPT-018a, OPT-018b, OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.11.yaml
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


def main():
    """Display tag usage statistics and frequency histogram"""

    print("Tag Usage Statistics")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"\nError loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Database path from config
    db_path = BASE_DIR / config['structure']['database_path']

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # OPT-074: Check if any tagged rules exist
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_count = cursor.fetchone()[0]

    if tagged_count == 0:
        # OPT-074: Empty-state reporting
        cursor.execute("SELECT COUNT(*) FROM rules")
        total_rules = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
        needs_tags_count = cursor.fetchone()[0]

        print("")
        print("No tagged rules in database.")
        print("")
        print(f"Database contains {total_rules} rules, {needs_tags_count} awaiting tag optimization.")
        print("")

        # Guidance message
        if total_rules == 0:
            print("Run 'make chatlogs-extract' to import rules from chatlogs first.")
        elif needs_tags_count > 0:
            print("Run 'make tags-optimize' or 'make tags-optimize-auto' to begin tagging.")
        else:
            print("All rules have empty tag lists. Check database integrity.")

        conn.close()
        return 0

    # OPT-012: Track tag reuse frequency across database
    cursor.execute("SELECT tags FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    all_tags = []

    for row in cursor.fetchall():
        tags_str = row[0]
        try:
            tags = json.loads(tags_str)
            all_tags.extend(tags)
        except json.JSONDecodeError:
            continue

    # OPT-013: Tag distribution histogram
    tag_counts = Counter(all_tags)

    # OPT-018a: Top 30 most frequent tags
    top_tags = tag_counts.most_common(30)

    # OPT-018b: Summary statistics
    total_unique_tags = len(tag_counts)
    total_tag_instances = sum(tag_counts.values())

    print("")
    print(f"Total unique tags: {total_unique_tags}")
    print(f"Total tag instances: {total_tag_instances}")
    print(f"Average tags per rule: {total_tag_instances / tagged_count:.1f}")
    print("")

    # OPT-018: Visual bar chart of tag frequency distribution
    if top_tags:
        max_count = top_tags[0][1]
        bar_width = 50  # Maximum bar width in characters

        print("Top 30 Most Frequent Tags:")
        print("")

        for tag, count in top_tags:
            # Calculate proportional bar length
            bar_length = int((count / max_count) * bar_width) if max_count > 0 else 0
            bar = '█' * bar_length

            # Format with tag name, bar, and count
            print(f"  {tag:30s} {bar:50s} {count:4d}")

    # OPT-014: Identify low-frequency tags for review
    print("")
    print("Low-Frequency Tags (used 1-2 times):")
    print("")

    low_freq_tags = [(tag, count) for tag, count in tag_counts.items() if count <= 2]
    low_freq_tags.sort(key=lambda x: (x[1], x[0]))  # Sort by count, then alphabetically

    if low_freq_tags:
        # Show first 20 low-frequency tags
        for tag, count in low_freq_tags[:20]:
            print(f"  {tag:30s} ({count} use{'s' if count > 1 else ''})")

        if len(low_freq_tags) > 20:
            print(f"  ... and {len(low_freq_tags) - 20} more")

        print("")
        print(f"Total low-frequency tags: {len(low_freq_tags)} ({len(low_freq_tags)/total_unique_tags*100:.1f}% of unique tags)")
    else:
        print("  (none)")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
