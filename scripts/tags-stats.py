#!/usr/bin/env python3
"""
Tag frequency histogram and statistics display

Implements constraints: OPT-001 through OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.10.yaml
"""

import sys
import json
import sqlite3
from pathlib import Path

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
    """Load deployment configuration and vocabulary."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


# ============================================================================
# RUNTIME-SCRIPT-TAG-OPTIMIZATION MODULE IMPLEMENTATION
# ============================================================================

def main():
    """Display tag frequency histogram and statistics."""
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path
    db_path = BASE_DIR / config['structure']['database_path']

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # OPT-074: Check if there are any tagged rules
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM rules")
    total_rules = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags_count = cursor.fetchone()[0]

    if tagged_count == 0:
        # OPT-074: Report empty state
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

        conn.close()
        return 0

    # OPT-012: Track tag reuse frequency
    cursor.execute("SELECT tags FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    all_tags = []
    for row in cursor.fetchall():
        tags = json.loads(row[0])
        all_tags.extend(tags)

    # Count tag frequencies
    tag_counts = {}
    for tag in all_tags:
        tag_counts[tag] = tag_counts.get(tag, 0) + 1

    # OPT-018a: Sort by frequency and get top 30
    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
    top_30 = sorted_tags[:30]

    # Calculate max frequency for bar scaling
    max_freq = top_30[0][1] if top_30 else 0
    bar_width = 50  # Maximum bar width in characters

    # OPT-018, OPT-018a: Display histogram with visual bars
    print("Tag Usage Statistics")
    print("="*70)
    print("")

    for tag, count in top_30:
        # Calculate proportional bar length
        bar_length = int((count / max_freq) * bar_width) if max_freq > 0 else 0
        bar = "█" * bar_length
        print(f"{tag:40s} {bar} {count}")

    print("")

    # OPT-018b: Summary statistics
    total_unique_tags = len(tag_counts)
    total_tag_instances = len(all_tags)

    print("="*70)
    print(f"Total unique tags: {total_unique_tags}")
    print(f"Total tag instances: {total_tag_instances}")
    print(f"Average tags per rule: {total_tag_instances / tagged_count:.1f}")

    # OPT-014: Identify low-frequency tags
    low_freq_tags = [tag for tag, count in tag_counts.items() if count == 1]
    if low_freq_tags:
        print(f"Low-frequency tags (used once): {len(low_freq_tags)}")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
