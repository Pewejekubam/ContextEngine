#!/usr/bin/env python3
"""
Interactive vocabulary curation tool for typo detection, synonym merging, and rare tag cleanup

Implements constraints: VOCAB-001 through VOCAB-038
Generated from: specs/modules/runtime-script-vocabulary-curation-v1.2.0.yaml
"""

import sys
import json
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

import sqlite3
import re


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein (edit) distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def get_database_statistics(db_path):
    """VOCAB-038: Query database statistics on startup."""
    if not db_path.exists():
        return {
            'total_rules': 0,
            'tagged_rules': 0,
            'unique_tags': 0
        }

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
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

        return {
            'total_rules': total_rules,
            'tagged_rules': tagged_rules,
            'unique_tags': unique_tags
        }
    except sqlite3.OperationalError:
        # Table doesn't exist
        return {
            'total_rules': 0,
            'tagged_rules': 0,
            'unique_tags': 0
        }
    finally:
        conn.close()


def detect_typos(vocab):
    """VOCAB-020: Detect typos using edit distance = 1."""
    typos = []

    for domain, tags in vocab.get('tier_2_tags', {}).items():
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def detect_rare_tags(db_path):
    """VOCAB-022: Detect rare tags (1-2 uses across all rules)."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
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
    except sqlite3.OperationalError:
        # No rules table or json_each not available
        rare_tags = []

    conn.close()
    return rare_tags


def merge_synonym(db_path, old_tag, new_tag):
    """VOCAB-023: Update affected rules when merging synonyms."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Find all rules containing old_tag
        cursor.execute("""
            SELECT id, tags
            FROM rules
            WHERE json_each.value = ?
        """, (old_tag,))

        affected_rules = cursor.fetchall()

        for rule_id, tags_json in affected_rules:
            tags = json.loads(tags_json)
            # Replace old_tag with new_tag
            tags = [new_tag if t == old_tag else t for t in tags]
            # Remove duplicates while preserving order
            seen = set()
            tags = [t for t in tags if not (t in seen or seen.add(t))]

            cursor.execute("""
                UPDATE rules
                SET tags = ?
                WHERE id = ?
            """, (json.dumps(tags), rule_id))

        conn.commit()
        return len(affected_rules)
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def remove_tag(db_path, tag_to_remove):
    """VOCAB-024: Remove tag and set tags_state='needs_tags' for affected rules."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Find all rules containing tag_to_remove
        cursor.execute("""
            SELECT id, tags
            FROM rules
            WHERE json_each.value = ?
        """, (tag_to_remove,))

        affected_rules = cursor.fetchall()

        for rule_id, tags_json in affected_rules:
            tags = json.loads(tags_json)
            tags = [t for t in tags if t != tag_to_remove]

            # VOCAB-024: Set needs_tags if tags now empty
            if len(tags) == 0:
                cursor.execute("""
                    UPDATE rules
                    SET tags = ?, tags_state = 'needs_tags'
                    WHERE id = ?
                """, (json.dumps(tags), rule_id))
            else:
                cursor.execute("""
                    UPDATE rules
                    SET tags = ?
                    WHERE id = ?
                """, (json.dumps(tags), rule_id))

        conn.commit()
        return len(affected_rules)
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def main():
    """Interactive vocabulary curation tool for typo detection, synonym merging, and rare tag cleanup"""
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
    db_path = BASE_DIR / "data" / "rules.db"

    # Check if database exists
    if not db_path.exists():
        print("\nNo database found. Run 'make chatlogs-extract' first.")
        return 1

    # VOCAB-038: Get database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-019: Load current vocabulary state from filesystem
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # VOCAB-020: Detect typos
    typos = detect_typos(vocab)

    # VOCAB-022: Detect rare tags
    rare_tags = detect_rare_tags(db_path)

    # VOCAB-036: Empty state reporting
    if len(typos) == 0 and len(rare_tags) == 0:
        print("\nNo vocabulary curation needed.")
        print("\nDatabase state:")
        print(f"  Total rules: {stats['total_rules']}")
        print(f"  Rules with tags: {stats['tagged_rules']}")
        print(f"  Unique tags: {stats['unique_tags']}")
        print()

        if stats['total_rules'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules first.")
        elif stats['tagged_rules'] == 0:
            print("No rules have tags yet. Run 'make tags-optimize' to begin tagging.")
        else:
            print("Vocabulary is healthy. No typos or rare tags detected.")

        return 0

    # Collect all issues
    issues = []

    for domain, tag1, tag2 in typos:
        issues.append({
            'type': 'typo',
            'domain': domain,
            'tag1': tag1,
            'tag2': tag2,
            'description': f"Potential typo in {domain}: '{tag1}' vs '{tag2}'"
        })

    for tag, count in rare_tags:
        issues.append({
            'type': 'rare',
            'tag': tag,
            'count': count,
            'description': f"Rare tag: '{tag}' (used {count} time{'s' if count > 1 else ''})"
        })

    # VOCAB-021: Show maximum 5 decisions per session
    decisions_made = 0
    max_decisions = 5

    for issue in issues:
        if decisions_made >= max_decisions:
            print(f"\nReached maximum of {max_decisions} decisions per session.")
            print("Re-run to see more issues.")
            break

        print(f"\n{issue['description']}")

        if issue['type'] == 'typo':
            print(f"1. Merge '{issue['tag1']}' into '{issue['tag2']}'")
            print(f"2. Merge '{issue['tag2']}' into '{issue['tag1']}'")
            print("3. Keep both (not a typo)")
            print("4. Skip")

            choice = input("Choice [1-4]: ").strip()

            if choice == '1':
                count = merge_synonym(db_path, issue['tag1'], issue['tag2'])
                print(f"✓ Merged {count} rules")
                # Remove from vocabulary
                vocab['tier_2_tags'][issue['domain']].remove(issue['tag1'])
                decisions_made += 1
            elif choice == '2':
                count = merge_synonym(db_path, issue['tag2'], issue['tag1'])
                print(f"✓ Merged {count} rules")
                # Remove from vocabulary
                vocab['tier_2_tags'][issue['domain']].remove(issue['tag2'])
                decisions_made += 1
            elif choice == '3':
                print("✓ Keeping both tags")
                decisions_made += 1

        elif issue['type'] == 'rare':
            print("1. Remove tag (will set affected rules to needs_tags)")
            print("2. Keep tag")
            print("3. Skip")

            choice = input("Choice [1-3]: ").strip()

            if choice == '1':
                count = remove_tag(db_path, issue['tag'])
                print(f"✓ Removed from {count} rules")
                # Remove from vocabulary
                for domain_tags in vocab['tier_2_tags'].values():
                    if issue['tag'] in domain_tags:
                        domain_tags.remove(issue['tag'])
                decisions_made += 1
            elif choice == '2':
                print("✓ Keeping tag")
                decisions_made += 1

    # Save vocabulary if changes were made
    if decisions_made > 0:
        # VOCAB-012: Block style, 2-space indent, unsorted keys
        with open(vocab_path, 'w') as f:
            yaml.dump(
                vocab,
                f,
                default_flow_style=False,
                sort_keys=False,
                indent=2,
                allow_unicode=True
            )
        print(f"\n✓ Vocabulary updated with {decisions_made} changes")
    else:
        print("\n✓ No changes made")

    return 0


if __name__ == '__main__':
    sys.exit(main())
