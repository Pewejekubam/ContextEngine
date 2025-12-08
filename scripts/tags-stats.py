#!/usr/bin/env python3
"""
Tag frequency histogram and statistics display

Implements constraints: OPT-012, OPT-013, OPT-014, OPT-018, OPT-018a, OPT-018b, OPT-074
Generated from: build/modules/runtime-script-tag-optimization.yaml
Version: v1.5.11
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


def get_database_statistics(db_path):
    """Get database statistics for empty-state reporting."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get total
    cursor.execute("SELECT COUNT(*) FROM rules")
    total = cursor.fetchone()[0]

    # Get needs_tags count
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags = cursor.fetchone()[0]

    conn.close()

    return {
        'total': total,
        'needs_tags': needs_tags
    }


def main():
    """OPT-012, OPT-013, OPT-014, OPT-018: Tag frequency histogram and statistics display."""

    # Load configuration
    config = load_config()

    # Database path from config['structure']['database_path']
    db_path = BASE_DIR / config['structure']['database_path']

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-074: Check if any tagged rules exist
    tagged_count = conn.execute(
        "SELECT COUNT(*) as c FROM rules WHERE tags IS NOT NULL AND tags != '[]'"
    ).fetchone()['c']

    if tagged_count == 0:
        # OPT-074: Empty-state reporting
        stats = get_database_statistics(db_path)

        print("Tag Usage Statistics")
        print("="*70)
        print("")
        print("No tagged rules in database.")
        print("")
        print(f"Database contains {stats['total']} rules, {stats['needs_tags']} awaiting tag optimization.")
        print("")

        if stats['total'] == 0:
            print("Run 'make chatlogs-extract' to import rules from chatlogs first.")
        elif stats['needs_tags'] > 0:
            print("Run 'make tags-optimize' or 'make tags-optimize-auto' to begin tagging.")
        else:
            print("All rules have empty tag lists. Check database integrity.")

        conn.close()
        return 0

    # OPT-012: Track tag reuse frequency across database
    cursor = conn.cursor()
    cursor.execute("SELECT tags FROM rules WHERE tags IS NOT NULL AND tags != '[]'")

    all_tags = []
    for row in cursor.fetchall():
        tags = json.loads(row[0])
        all_tags.extend(tags)

    conn.close()

    # OPT-013: Calculate tag distribution
    tag_counts = Counter(all_tags)

    # OPT-018b: Summary statistics
    total_unique_tags = len(tag_counts)
    total_tag_instances = sum(tag_counts.values())

    # OPT-018a: Top 30 most frequent tags with proportional bars
    top_tags = tag_counts.most_common(30)

    # Calculate max count for bar scaling
    max_count = top_tags[0][1] if top_tags else 0
    bar_width = 50  # Max bar width in characters

    # Print report
    print("Tag Usage Statistics")
    print("="*70)
    print("")
    print(f"Total unique tags: {total_unique_tags}")
    print(f"Total tag instances: {total_tag_instances}")
    print(f"Average tags per rule: {total_tag_instances / tagged_count:.1f}")
    print("")
    print("Top 30 Most Frequent Tags:")
    print("-"*70)

    for tag, count in top_tags:
        # OPT-018: Visual bar chart
        bar_length = int((count / max_count) * bar_width) if max_count > 0 else 0
        bar = '█' * bar_length
        percentage = (count / total_tag_instances) * 100

        # Format: tag (count) [bar] percentage
        print(f"{tag:30s} {count:4d} {bar:50s} {percentage:5.1f}%")

    # OPT-014: Identify low-frequency tags for review
    low_frequency_tags = [tag for tag, count in tag_counts.items() if count == 1]

    if low_frequency_tags:
        print("")
        print("-"*70)
        print(f"\nLow-frequency tags (used only once): {len(low_frequency_tags)}")
        print("Consider reviewing these tags for potential consolidation:")
        print("")

        # Show first 20 low-frequency tags
        for i, tag in enumerate(sorted(low_frequency_tags)[:20]):
            print(f"  • {tag}")

        if len(low_frequency_tags) > 20:
            print(f"  ... and {len(low_frequency_tags) - 20} more")

    return 0


if __name__ == '__main__':
    sys.exit(main())
