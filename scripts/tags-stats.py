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
import argparse

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

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Display tag usage statistics and frequency histogram'
    )
    args = parser.parse_args()

    print("Tag Usage Statistics")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path from config
    db_path = BASE_DIR / config['structure']['database_path']

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-074: Check for empty state
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_count = cursor.fetchone()[0]

    if tagged_count == 0:
        # OPT-074: Empty state reporting
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
    rows = cursor.fetchall()

    tag_counter = Counter()
    for row in rows:
        tags_json = row['tags']
        try:
            tags = json.loads(tags_json)
            tag_counter.update(tags)
        except (json.JSONDecodeError, TypeError):
            # Skip malformed tag data
            pass

    # OPT-018a: Top 30 most frequent tags
    top_tags = tag_counter.most_common(30)

    if not top_tags:
        print("\nNo tags found in database.")
        conn.close()
        return 0

    # OPT-018: Visual bar chart of tag frequency distribution
    max_count = top_tags[0][1] if top_tags else 0
    max_bar_width = 50  # Maximum bar width in characters

    print("")
    for tag, count in top_tags:
        # Calculate proportional bar width
        bar_width = int((count / max_count) * max_bar_width) if max_count > 0 else 0
        bar = '█' * bar_width
        print(f"  {tag:30s} {count:4d} {bar}")

    # OPT-018b: Summary statistics
    print("")
    print("="*70)
    print(f"  Total unique tags: {len(tag_counter)}")
    print(f"  Total tag instances: {sum(tag_counter.values())}")
    print(f"  Rules with tags: {tagged_count}")
    print(f"  Average tags per rule: {sum(tag_counter.values()) / tagged_count:.1f}")

    # OPT-014: Identify low-frequency tags for review
    low_frequency_tags = [tag for tag, count in tag_counter.items() if count == 1]
    if low_frequency_tags:
        print(f"  Tags used only once: {len(low_frequency_tags)}")
        if len(low_frequency_tags) <= 10:
            print(f"    {', '.join(sorted(low_frequency_tags))}")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
