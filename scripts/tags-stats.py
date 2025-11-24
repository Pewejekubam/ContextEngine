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


def get_tag_frequency(db_path):
    """Track tag reuse frequency across database (OPT-012)."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get all rules with tags
    cursor.execute("""
        SELECT tags FROM rules
        WHERE tags IS NOT NULL AND tags != '[]'
    """)

    # Count tag frequency
    tag_counter = Counter()
    for (tags_json,) in cursor.fetchall():
        tags = json.loads(tags_json)
        tag_counter.update(tags)

    conn.close()
    return tag_counter


def get_database_statistics(db_path):
    """Get database statistics for empty-state reporting."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get total rules
    cursor.execute("SELECT COUNT(*) FROM rules")
    total = cursor.fetchone()[0]

    # Get count needing tags
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags_count = cursor.fetchone()[0]

    # Get count with tags
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_count = cursor.fetchone()[0]

    conn.close()

    return {
        'total': total,
        'needs_tags': needs_tags_count,
        'tagged': tagged_count
    }


def display_histogram(tag_counter, max_display=30):
    """Display visual bar chart of tag frequency distribution (OPT-018, OPT-018a)."""
    if not tag_counter:
        return

    # OPT-018a: Show top 30 most frequent tags
    most_common = tag_counter.most_common(max_display)

    # Calculate bar width based on max frequency
    max_freq = most_common[0][1]
    bar_width = 50  # Maximum bar length in characters

    print("\nTop {} Most Frequent Tags:".format(min(len(most_common), max_display)))
    print("=" * 70)

    for tag, count in most_common:
        # Calculate proportional bar length
        bar_length = int((count / max_freq) * bar_width)
        bar = '█' * bar_length

        # Right-align count for visual consistency
        print(f"  {tag:30s} {bar} {count:3d}")


def identify_low_frequency_tags(tag_counter, threshold=2):
    """Identify low-frequency tags for review (OPT-014)."""
    low_freq_tags = [(tag, count) for tag, count in tag_counter.items() if count <= threshold]
    return sorted(low_freq_tags, key=lambda x: x[1])


def main():
    """Display tag frequency histogram and statistics."""
    print("Tag Usage Statistics")
    print("=" * 70)

    # Load configuration
    config = load_config()

    # Get database path
    db_path = BASE_DIR / config['structure']['database_path']

    # Check if database exists
    if not db_path.exists():
        print("\nError: Database not found at", db_path, file=sys.stderr)
        return 1

    # Get database statistics
    stats = get_database_statistics(db_path)

    # OPT-074: Check for empty state
    if stats['tagged'] == 0:
        print("\nNo tagged rules in database.")
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

    # OPT-012, OPT-013: Get tag frequency distribution
    tag_counter = get_tag_frequency(db_path)

    if not tag_counter:
        print("\nNo tags found in database.")
        return 0

    # OPT-018b: Display summary statistics
    total_unique_tags = len(tag_counter)
    total_tag_instances = sum(tag_counter.values())

    print(f"\nTotal unique tags: {total_unique_tags}")
    print(f"Total tag instances: {total_tag_instances}")
    print(f"Average tags per rule: {total_tag_instances / stats['tagged']:.1f}")

    # OPT-018, OPT-018a: Display histogram
    display_histogram(tag_counter)

    # OPT-014: Identify low-frequency tags
    low_freq_tags = identify_low_frequency_tags(tag_counter, threshold=2)

    if low_freq_tags:
        print("\n" + "=" * 70)
        print(f"Low-Frequency Tags (used ≤2 times): {len(low_freq_tags)} tags")
        print("=" * 70)

        # Show first 20 low-frequency tags
        display_count = min(20, len(low_freq_tags))
        for tag, count in low_freq_tags[:display_count]:
            print(f"  {tag:30s} {count:3d}")

        if len(low_freq_tags) > display_count:
            remaining = len(low_freq_tags) - display_count
            print(f"\n  ... and {remaining} more")

        print("\nNote: Low-frequency tags may indicate:")
        print("  - Overly specific tags that could be generalized")
        print("  - Typos or inconsistent naming")
        print("  - Emerging concepts needing more rules")

    return 0


if __name__ == '__main__':
    sys.exit(main())
