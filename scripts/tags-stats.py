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
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
CONFIG_PATH = BASE_DIR / "config" / "deployment.yaml"

# Load config to get project root and context engine home
with open(CONFIG_PATH) as f:
    _config = yaml.safe_load(f)
    PROJECT_ROOT = Path(_config['paths']['project_root'])
    BASE_DIR = Path(_config['paths']['context_engine_home'])


def load_config():
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def get_tag_statistics(db_path):
    """
    Calculate tag frequency statistics from database.

    Implements:
    - OPT-012: Track tag reuse frequency across database
    - OPT-013: Report tag distribution histogram
    - OPT-014: Identify low-frequency tags for review
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all rules with tags
    cursor.execute("SELECT tags FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    rules = cursor.fetchall()

    if not rules:
        # OPT-074: No tagged rules - return None to trigger empty state message
        return None

    # Count tag frequencies (OPT-012)
    tag_counter = Counter()
    for rule in rules:
        tags_list = json.loads(rule['tags'])
        for tag in tags_list:
            tag_counter[tag] += 1

    conn.close()
    return tag_counter


def display_histogram(tag_counter):
    """
    Display visual histogram of tag frequency distribution.

    Implements:
    - OPT-018: Visual bar chart of tag frequency distribution
    - OPT-018a: Top 30 most frequent tags with proportional bars
    - OPT-018b: Summary with total unique tags and total instances
    """
    print("Tag Usage Statistics")
    print("="*70)
    print()

    # OPT-018a: Top 30 most frequent tags
    top_tags = tag_counter.most_common(30)

    if not top_tags:
        print("No tags found in database.")
        return

    # Find max frequency for bar scaling
    max_freq = top_tags[0][1]

    # Calculate bar width (proportional to frequency)
    max_bar_width = 50

    # Display bars
    for tag, count in top_tags:
        bar_width = int((count / max_freq) * max_bar_width)
        bar = '█' * bar_width
        print(f"  {tag:30s} {bar} {count}")

    # OPT-018b: Summary statistics
    print()
    print("="*70)
    print(f"Total unique tags: {len(tag_counter)}")
    print(f"Total tag instances: {sum(tag_counter.values())}")

    # OPT-014: Identify low-frequency tags for review
    low_freq_tags = [tag for tag, count in tag_counter.items() if count == 1]
    if low_freq_tags:
        print(f"Low-frequency tags (used once): {len(low_freq_tags)}")

        # Show first 10 low-frequency tags as examples
        if len(low_freq_tags) <= 10:
            print(f"  Examples: {', '.join(low_freq_tags)}")
        else:
            print(f"  Examples: {', '.join(low_freq_tags[:10])}, ... (and {len(low_freq_tags) - 10} more)")


def get_database_statistics(db_path):
    """Get database statistics for empty-state reporting."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get counts by tags_state
    cursor.execute("""
        SELECT tags_state, COUNT(*) as count
        FROM rules
        GROUP BY tags_state
    """)
    state_counts = dict(cursor.fetchall())

    # Get total
    cursor.execute("SELECT COUNT(*) FROM rules")
    total = cursor.fetchone()[0]

    conn.close()

    return {
        'total': total,
        'curated': state_counts.get('curated', 0),
        'refined': state_counts.get('refined', 0),
        'pending_review': state_counts.get('pending_review', 0),
        'needs_tags': state_counts.get('needs_tags', 0)
    }


def main():
    """Display tag frequency histogram and statistics."""
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path from config
    db_path = BASE_DIR / config['structure']['database_path']

    # Get tag statistics
    tag_counter = get_tag_statistics(db_path)

    if tag_counter is None:
        # OPT-074: Empty state reporting
        stats = get_database_statistics(db_path)

        print("Tag Usage Statistics")
        print("="*70)
        print()
        print("No tagged rules in database.")
        print()
        print(f"Database contains {stats['total']} rules, {stats['needs_tags']} awaiting tag optimization.")
        print()

        if stats['total'] == 0:
            print("Run 'make chatlogs-extract' to import rules from chatlogs first.")
        elif stats['needs_tags'] > 0:
            print("Run 'make tags-optimize' or 'make tags-optimize-auto' to begin tagging.")
        else:
            print("All rules have empty tag lists. Check database integrity.")

        return 0

    # Display histogram
    display_histogram(tag_counter)

    return 0


if __name__ == '__main__':
    sys.exit(main())
