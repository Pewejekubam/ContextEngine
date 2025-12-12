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


def get_database_statistics(db_path):
    """VOCAB-038: Query database statistics using shared helper function."""
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
    """Calculate Levenshtein edit distance between two strings."""
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


def detect_typos(vocab_path):
    """VOCAB-020: Detect typos using edit distance = 1."""
    # VOCAB-019: Query current vocabulary state from filesystem
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    tier_2_tags = vocab.get('tier_2_tags', {})
    typos = []

    # Compare all tier-2 tags within each domain
    for domain, tags in tier_2_tags.items():
        if not tags:
            continue
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def detect_rare_tags(db_path):
    """VOCAB-022: Detect rare tags (1-2 uses across all rules in database)."""
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


def update_rule_tags(db_path, rule_id, old_tag, new_tag):
    """VOCAB-023: Update affected rules when merging synonyms."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get current tags
    cursor.execute("SELECT tags FROM rules WHERE id = ?", (rule_id,))
    result = cursor.fetchone()
    if not result:
        conn.close()
        return False

    tags = json.loads(result[0])

    # Replace old tag with new tag
    if old_tag in tags:
        tags.remove(old_tag)
        if new_tag not in tags:
            tags.append(new_tag)

        # Update rule
        cursor.execute(
            "UPDATE rules SET tags = ? WHERE id = ?",
            (json.dumps(tags), rule_id)
        )
        conn.commit()

    conn.close()
    return True


def remove_tag_from_rule(db_path, rule_id, tag_to_remove):
    """VOCAB-024: Remove tag and set tags_state='needs_tags' if all tags removed."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get current tags
    cursor.execute("SELECT tags FROM rules WHERE id = ?", (rule_id,))
    result = cursor.fetchone()
    if not result:
        conn.close()
        return False

    tags = json.loads(result[0])

    # Remove tag
    if tag_to_remove in tags:
        tags.remove(tag_to_remove)

        # VOCAB-024: Set tags_state='needs_tags' if tags empty
        if not tags:
            cursor.execute(
                "UPDATE rules SET tags = ?, tags_state = 'needs_tags' WHERE id = ?",
                (json.dumps(tags), rule_id)
            )
        else:
            cursor.execute(
                "UPDATE rules SET tags = ? WHERE id = ?",
                (json.dumps(tags), rule_id)
            )
        conn.commit()

    conn.close()
    return True


def find_rules_with_tag(db_path, tag):
    """Find all rules containing a specific tag."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id
        FROM rules
        WHERE json_extract(tags, '$') LIKE ?
    """, (f'%"{tag}"%',))

    rule_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    return rule_ids


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

    # Get paths
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    db_path = Path(config['structure']['database_path'])

    # Get database statistics (VOCAB-038)
    stats = get_database_statistics(db_path)

    # Detect issues
    typos = detect_typos(vocab_path)
    rare_tags = detect_rare_tags(db_path)

    # VOCAB-036: Report empty state when no curation needed
    if len(typos) == 0 and len(rare_tags) == 0:
        print("\nNo vocabulary curation needed.\n")
        print("Database state:")
        print(f"  Total rules: {stats['total_rules']}")
        print(f"  Rules with tags: {stats['tagged_rules']}")
        print(f"  Unique tags: {stats['unique_tags']}\n")

        # Guidance message
        if stats['total_rules'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules first.")
        elif stats['tagged_rules'] == 0:
            print("No rules have tags yet. Run 'make tags-optimize' to begin tagging.")
        else:
            print("Vocabulary is healthy. No typos or rare tags detected.")

        return 0

    # Collect issues (VOCAB-021: max 5 decisions per session)
    issues = []

    for domain, tag1, tag2 in typos[:5]:
        issues.append({
            'type': 'typo',
            'domain': domain,
            'tag1': tag1,
            'tag2': tag2
        })

    remaining_slots = 5 - len(issues)
    for tag, count in rare_tags[:remaining_slots]:
        issues.append({
            'type': 'rare',
            'tag': tag,
            'count': count
        })

    # Interactive review
    decisions_made = 0
    for i, issue in enumerate(issues):
        print(f"\n[{i+1}/{len(issues)}]", end=" ")

        if issue['type'] == 'typo':
            print(f"Potential typo in '{issue['domain']}' domain:")
            print(f"  Tags: '{issue['tag1']}' and '{issue['tag2']}'")
            print("\nActions:")
            print("  1) Merge (keep one, update rules)")
            print("  2) Keep both (not a typo)")
            print("  3) Skip")

            choice = input("\nChoice [1-3]: ").strip()

            if choice == '1':
                print(f"\nWhich to keep?")
                print(f"  1) {issue['tag1']}")
                print(f"  2) {issue['tag2']}")
                keep_choice = input("\nChoice [1-2]: ").strip()

                if keep_choice in ['1', '2']:
                    old_tag = issue['tag2'] if keep_choice == '1' else issue['tag1']
                    new_tag = issue['tag1'] if keep_choice == '1' else issue['tag2']

                    # Update all rules
                    rule_ids = find_rules_with_tag(db_path, old_tag)
                    for rule_id in rule_ids:
                        update_rule_tags(db_path, rule_id, old_tag, new_tag)

                    print(f"\nMerged '{old_tag}' → '{new_tag}' ({len(rule_ids)} rules updated)")
                    decisions_made += 1

        elif issue['type'] == 'rare':
            print(f"Rare tag: '{issue['tag']}' (used {issue['count']} time(s))")
            print("\nActions:")
            print("  1) Remove tag from vocabulary and rules")
            print("  2) Keep tag")
            print("  3) Skip")

            choice = input("\nChoice [1-3]: ").strip()

            if choice == '1':
                # Remove from all rules
                rule_ids = find_rules_with_tag(db_path, issue['tag'])
                for rule_id in rule_ids:
                    remove_tag_from_rule(db_path, rule_id, issue['tag'])

                print(f"\nRemoved '{issue['tag']}' from {len(rule_ids)} rule(s)")
                decisions_made += 1

    # Summary
    print(f"\n{'='*70}")
    print(f"Review complete: {decisions_made} decision(s) made")

    if len(typos) > 5 or len(rare_tags) > (5 - len(typos)):
        print("\nMore issues available. Re-run to see next batch.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
