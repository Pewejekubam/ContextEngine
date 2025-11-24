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
# HELPER FUNCTIONS
# ============================================================================

def collect_tag_frequencies(db_path):
    """Collect tag usage frequencies across all rules (OPT-012)."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get all tagged rules
    cursor.execute("SELECT tags FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    rows = cursor.fetchall()
    conn.close()

    # Count tag frequencies
    tag_counts = {}
    for (tags_json,) in rows:
        try:
            tags = json.loads(tags_json)
            if isinstance(tags, list):
                for tag in tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
        except json.JSONDecodeError:
            continue

    return tag_counts


def display_histogram(tag_counts):
    """Display visual histogram of tag frequencies (OPT-018, OPT-018a)."""
    if not tag_counts:
        return

    # Sort by frequency (descending)
    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)

    # OPT-018a: Show top 30
    top_tags = sorted_tags[:30]

    # Find max frequency for bar scaling
    max_freq = max(count for _, count in top_tags)
    bar_width = 50  # Maximum bar width in characters

    print("\nTag Frequency Distribution")
    print("="*70)

    for tag, count in top_tags:
        # Calculate proportional bar length
        bar_len = int((count / max_freq) * bar_width)
        bar = '█' * bar_len

        print(f"{tag:30s} {bar} {count}")

    # OPT-018b: Summary statistics
    total_unique = len(tag_counts)
    total_instances = sum(tag_counts.values())

    print(f"\n{'='*70}")
    print(f"Summary:")
    print(f"  Total unique tags: {total_unique}")
    print(f"  Total tag instances: {total_instances}")
    print(f"  Average tags per instance: {total_instances / total_unique:.2f}")


def identify_low_frequency_tags(tag_counts, threshold=2):
    """Identify tags used infrequently (OPT-014)."""
    low_freq_tags = {tag: count for tag, count in tag_counts.items() if count <= threshold}

    if low_freq_tags:
        print(f"\nLow-Frequency Tags (used ≤ {threshold} times):")
        print("="*70)

        sorted_low_freq = sorted(low_freq_tags.items(), key=lambda x: x[1], reverse=True)
        for tag, count in sorted_low_freq:
            print(f"  {tag}: {count} use{'s' if count > 1 else ''}")

        print(f"\nTotal low-frequency tags: {len(low_freq_tags)} ({len(low_freq_tags)/len(tag_counts)*100:.1f}%)")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Display tag frequency histogram and statistics (OPT-012, OPT-013, OPT-014, OPT-018)."""
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

    # OPT-074: Check if any tagged rules exist
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_count = cursor.fetchone()[0]

    if tagged_count == 0:
        # OPT-074: Empty state reporting
        cursor.execute("SELECT COUNT(*) FROM rules")
        total_rules = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
        needs_tags_count = cursor.fetchone()[0]
        conn.close()

        print(f"\nNo tagged rules in database.\n")
        print(f"Database contains {total_rules} rules, {needs_tags_count} awaiting tag optimization.\n")

        if total_rules == 0:
            print("Run 'make chatlogs-extract' to import rules from chatlogs first.")
        elif needs_tags_count > 0:
            print("Run 'make tags-optimize' or 'make tags-optimize-auto' to begin tagging.")
        else:
            print("All rules have empty tag lists. Check database integrity.")

        return 0

    conn.close()

    # OPT-012: Collect tag frequencies
    tag_counts = collect_tag_frequencies(db_path)

    if not tag_counts:
        print("\nNo tags found in database.")
        return 0

    # OPT-013, OPT-018: Display histogram
    display_histogram(tag_counts)

    # OPT-014: Identify low-frequency tags
    identify_low_frequency_tags(tag_counts, threshold=2)

    return 0


if __name__ == '__main__':
    sys.exit(main())
