#!/usr/bin/env python3
"""
Interactive vocabulary curation tool for typo detection, synonym merging, and rare tag cleanup

Implements constraints: VOCAB-001 through VOCAB-038
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


# ============================================================================
# RUNTIME-SCRIPT-VOCABULARY-CURATION MODULE IMPLEMENTATION
# ============================================================================


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein edit distance between two strings (VOCAB-020)."""
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


def get_database_statistics(db_path):
    """Query database statistics (VOCAB-038)."""
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


def detect_typos(vocabulary):
    """Detect typos using edit distance = 1 (VOCAB-020)."""
    typos = []
    tier_2_tags = vocabulary.get('tier_2_tags', {})

    for domain, tags in tier_2_tags.items():
        # Compare all tags within each domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def detect_rare_tags(db_path):
    """Detect rare tags (1-2 uses) across all rules (VOCAB-022)."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Query rare tags
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
    """Vocabulary curation workflows: typo detection, synonym merging, rare tag cleanup, and pre-commit health checks"""
    print("Vocabulary Review")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-019: Load vocabulary from filesystem (not cached)
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    try:
        with open(vocab_path) as f:
            vocabulary = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path
    db_path = BASE_DIR / "data" / "rules.db"

    # VOCAB-038: Query database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-020: Detect typos
    typos = detect_typos(vocabulary)

    # VOCAB-022: Detect rare tags
    rare_tags = detect_rare_tags(db_path)

    # VOCAB-036: Empty state reporting
    if len(typos) == 0 and len(rare_tags) == 0:
        print("\nNo vocabulary curation needed.")
        print("\nDatabase state:")
        print(f"  Total rules: {stats['total_rules']}")
        print(f"  Rules with tags: {stats['tagged_rules']}")
        print(f"  Unique tags: {stats['unique_tags']}")

        # Guidance message based on state
        if stats['total_rules'] == 0:
            print("\nDatabase is empty. Run 'make chatlogs-extract' to import rules first.")
        elif stats['tagged_rules'] == 0:
            print("\nNo rules have tags yet. Run 'make tags-optimize' to begin tagging.")
        else:
            print("\nVocabulary is healthy. No typos or rare tags detected.")

        return 0

    # VOCAB-021: Show maximum 5 decisions per session
    issues = []

    # Add typos to issues
    for domain, tag1, tag2 in typos:
        issues.append({
            'type': 'typo',
            'domain': domain,
            'tag1': tag1,
            'tag2': tag2
        })

    # Add rare tags to issues
    for tag, count in rare_tags:
        issues.append({
            'type': 'rare',
            'tag': tag,
            'count': count
        })

    # Limit to 5 decisions
    issues_to_show = issues[:5]

    print(f"\nFound {len(issues)} issue(s). Showing first {len(issues_to_show)}:")
    print("\nIssues detected:")

    for i, issue in enumerate(issues_to_show, 1):
        if issue['type'] == 'typo':
            print(f"  {i}. Potential typo in '{issue['domain']}': '{issue['tag1']}' vs '{issue['tag2']}'")
        elif issue['type'] == 'rare':
            print(f"  {i}. Rare tag: '{issue['tag']}' (used {issue['count']} time(s))")

    if len(issues) > 5:
        print(f"\n{len(issues) - 5} more issue(s) found. Re-run to see more after addressing these.")

    print("\nNote: This is a review tool. Manual curation required.")
    print("To merge synonyms or remove tags:")
    print("  1. Edit config/tag-vocabulary.yaml")
    print("  2. Update affected rules in database")
    print("  3. Set tags_state='needs_tags' for rules with removed tags (VOCAB-024)")

    return 0


if __name__ == '__main__':
    sys.exit(main())
