#!/usr/bin/env python3
"""
Interactive vocabulary curation tool for typo detection, synonym merging, and rare tag cleanup

Implements constraints: VOCAB-019, VOCAB-020, VOCAB-021, VOCAB-022, VOCAB-023, VOCAB-024, VOCAB-036, VOCAB-038
Generated from: specs/modules/runtime-script-vocabulary-curation-v1.2.0.yaml
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


def get_database_statistics(db_path):
    """VOCAB-038: Query database statistics for reporting."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Total rules
    cursor.execute("SELECT COUNT(*) FROM rules")
    total_rules = cursor.fetchone()[0]

    # Rules with non-empty tags
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags IS NOT NULL AND tags != '[]'")
    tagged_rules = cursor.fetchone()[0]

    # Unique tags (requires JSON extraction)
    try:
        cursor.execute("""
            SELECT COUNT(DISTINCT json_each.value)
            FROM rules, json_each(rules.tags)
            WHERE rules.tags IS NOT NULL AND rules.tags != '[]'
        """)
        unique_tags = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        unique_tags = 0

    conn.close()

    return {
        'total_rules': total_rules,
        'tagged_rules': tagged_rules,
        'unique_tags': unique_tags
    }


def levenshtein_distance(s1, s2):
    """VOCAB-020: Calculate Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and current_row are one character longer than s2
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def detect_typos(vocab):
    """VOCAB-020: Detect typos using edit distance = 1."""
    typos = []

    for domain, tags in vocab.get('tier_2_tags', {}).items():
        # Compare all pairs within domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def detect_rare_tags(db_path):
    """VOCAB-022: Detect rare tags (1-2 uses across all rules in database)."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tag, COUNT(*) as usage_count
        FROM (
            SELECT json_each.value as tag
            FROM rules, json_each(rules.tags)
        )
        GROUP BY tag
        HAVING usage_count <= 2
        ORDER BY usage_count ASC, tag ASC
    """)

    rare_tags = cursor.fetchall()
    conn.close()

    return rare_tags


def main():
    """Interactive vocabulary curation tool for typo detection, synonym merging, and rare tag cleanup."""
    print("Vocabulary Review")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-019: Query current vocabulary state from filesystem
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # Get database path
    db_path = BASE_DIR / "data" / "rules.db"

    # VOCAB-038: Get database statistics
    try:
        stats = get_database_statistics(db_path)
    except Exception as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-020: Detect typos
    typos = detect_typos(vocab)

    # VOCAB-022: Detect rare tags
    try:
        rare_tags = detect_rare_tags(db_path)
    except Exception as e:
        print(f"Error detecting rare tags: {e}", file=sys.stderr)
        rare_tags = []

    # VOCAB-036: Report empty state when no curation needed
    if len(typos) == 0 and len(rare_tags) == 0:
        print("\nNo vocabulary curation needed.")
        print("\nDatabase state:")
        print(f"  Total rules: {stats['total_rules']}")
        print(f"  Rules with tags: {stats['tagged_rules']}")
        print(f"  Unique tags: {stats['unique_tags']}")
        print()

        # VOCAB-036: Guidance based on database state
        if stats['total_rules'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules first.")
        elif stats['tagged_rules'] == 0:
            print("No rules have tags yet. Run 'make tags-optimize' to begin tagging.")
        else:
            print("Vocabulary is healthy. No typos or rare tags detected.")

        return 0

    # VOCAB-021: Show maximum 5 decisions per session
    decisions_shown = 0
    max_decisions = 5

    # Show typos first
    if typos:
        print(f"\nPotential typos detected (edit distance = 1):")
        for domain, tag1, tag2 in typos[:max_decisions - decisions_shown]:
            print(f"\n  Domain: {domain}")
            print(f"  Tags: '{tag1}' vs '{tag2}'")
            decisions_shown += 1
            if decisions_shown >= max_decisions:
                break

    # Show rare tags if we haven't hit limit
    if rare_tags and decisions_shown < max_decisions:
        print(f"\nRare tags (used 1-2 times):")
        for tag, count in rare_tags[:max_decisions - decisions_shown]:
            print(f"  '{tag}': {count} uses")
            decisions_shown += 1
            if decisions_shown >= max_decisions:
                break

    # Show database statistics
    print(f"\nDatabase state:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Rules with tags: {stats['tagged_rules']}")
    print(f"  Unique tags: {stats['unique_tags']}")

    # Inform user if more issues exist
    total_issues = len(typos) + len(rare_tags)
    if total_issues > decisions_shown:
        print(f"\nShowing {decisions_shown} of {total_issues} issues. Re-run to see more.")

    print("\nNote: This is a detection-only tool. Manual curation required.")
    print("      To merge tags, edit config/tag-vocabulary.yaml and update database manually.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
