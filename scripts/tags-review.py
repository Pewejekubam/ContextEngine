#!/usr/bin/env python3
"""
Interactive vocabulary curation tool for typo detection, synonym merging, and rare tag cleanup

Implements constraints: VOCAB-019 through VOCAB-024, VOCAB-036, VOCAB-038
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
    """
    Query database statistics (VOCAB-038).

    Returns:
        dict with total_rules, tagged_rules, unique_tags
    """
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
    """
    Calculate Levenshtein edit distance between two strings.

    Used for VOCAB-020 typo detection (edit distance = 1).
    """
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
    """
    Detect typos using edit distance = 1 (VOCAB-020).

    Returns:
        List of (tag1, tag2, domain) tuples for potential typos
    """
    typos = []

    # Check within each domain
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if not isinstance(tags, list):
            continue

        # Compare all pairs within domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((tag1, tag2, domain))

    return typos


def detect_rare_tags(db_path):
    """
    Detect rare tags (1-2 uses across all rules) (VOCAB-022).

    Returns:
        List of (tag, usage_count) tuples
    """
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # VOCAB-022 query
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


def update_rule_tags(db_path, old_tag, new_tag):
    """
    Update affected rules when merging synonyms (VOCAB-023).

    Replaces old_tag with new_tag in tags JSON array.
    """
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Find rules with old_tag
    cursor.execute("""
        SELECT id, tags
        FROM rules
        WHERE json_type(tags, '$') = 'array'
    """)

    updated_count = 0
    for rule_id, tags_json in cursor.fetchall():
        tags = json.loads(tags_json)

        if old_tag in tags:
            # Replace old with new
            tags = [new_tag if t == old_tag else t for t in tags]
            # Remove duplicates while preserving order
            seen = set()
            tags = [t for t in tags if not (t in seen or seen.add(t))]

            cursor.execute(
                "UPDATE rules SET tags = ? WHERE id = ?",
                (json.dumps(tags), rule_id)
            )
            updated_count += 1

    conn.commit()
    conn.close()

    return updated_count


def remove_tag(db_path, tag_to_remove):
    """
    Remove tag from all rules and set tags_state='needs_tags' (VOCAB-024).
    """
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Find rules with the tag
    cursor.execute("""
        SELECT id, tags
        FROM rules
        WHERE json_type(tags, '$') = 'array'
    """)

    updated_count = 0
    for rule_id, tags_json in cursor.fetchall():
        tags = json.loads(tags_json)

        if tag_to_remove in tags:
            tags = [t for t in tags if t != tag_to_remove]

            # VOCAB-024: Set tags_state='needs_tags' for rules with removed tags
            cursor.execute(
                "UPDATE rules SET tags = ?, tags_state = 'needs_tags' WHERE id = ?",
                (json.dumps(tags), rule_id)
            )
            updated_count += 1

    conn.commit()
    conn.close()

    return updated_count


def main():
    """Interactive vocabulary curation: typo detection, synonym merging, rare tag cleanup"""
    print("Vocabulary Review")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get paths
    db_path = BASE_DIR / "data" / "rules.db"
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"

    # Check database exists
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-038: Get database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-019: Load current vocabulary state from filesystem
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # Detect issues
    typos = detect_typos(vocab)
    rare_tags = detect_rare_tags(db_path)

    # VOCAB-036: Report empty state if no curation needed
    if len(typos) == 0 and len(rare_tags) == 0:
        print("\nNo vocabulary curation needed.")
        print(f"\nDatabase state:")
        print(f"  Total rules: {stats['total_rules']}")
        print(f"  Rules with tags: {stats['tagged_rules']}")
        print(f"  Unique tags: {stats['unique_tags']}")

        if stats['total_rules'] == 0:
            print("\nDatabase is empty. Run 'make chatlogs-extract' to import rules first.")
        elif stats['tagged_rules'] == 0:
            print("\nNo rules have tags yet. Run 'make tags-optimize' to begin tagging.")
        else:
            print("\nVocabulary is healthy. No typos or rare tags detected.")

        return 0

    # VOCAB-021: Show maximum 5 decisions per session
    issues = []

    # Add typos
    for tag1, tag2, domain in typos[:5]:
        issues.append(('typo', tag1, tag2, domain))

    # Add rare tags (if space remains)
    remaining = 5 - len(issues)
    for tag, count in rare_tags[:remaining]:
        issues.append(('rare', tag, count, None))

    print(f"\nFound {len(typos)} potential typos and {len(rare_tags)} rare tags.")
    print(f"Showing {len(issues)} issues (maximum 5 per session).")
    print("\nRe-run this script to see more issues.\n")

    # Present issues
    decisions_made = 0
    for issue in issues:
        if issue[0] == 'typo':
            _, tag1, tag2, domain = issue
            print(f"\nPotential typo in domain '{domain}':")
            print(f"  '{tag1}' vs '{tag2}'")
            print("\nOptions:")
            print("  1. Merge (keep first, replace second)")
            print("  2. Keep both (not a typo)")
            print("  3. Skip for now")

            choice = input("\nYour choice (1-3): ").strip()

            if choice == '1':
                # Merge: replace tag2 with tag1
                count = update_rule_tags(db_path, tag2, tag1)

                # Remove tag2 from vocabulary
                if domain in vocab.get('tier_2_tags', {}):
                    if tag2 in vocab['tier_2_tags'][domain]:
                        vocab['tier_2_tags'][domain].remove(tag2)

                print(f"✓ Merged: {count} rules updated, removed '{tag2}' from vocabulary")
                decisions_made += 1

            elif choice == '2':
                print("✓ Kept both tags")
            else:
                print("⊘ Skipped")

        elif issue[0] == 'rare':
            _, tag, count, _ = issue
            print(f"\nRare tag (used {count} times): '{tag}'")
            print("\nOptions:")
            print("  1. Remove from vocabulary")
            print("  2. Keep (intentionally rare)")
            print("  3. Skip for now")

            choice = input("\nYour choice (1-3): ").strip()

            if choice == '1':
                # Remove from all domains
                removed_count = remove_tag(db_path, tag)

                for domain in vocab.get('tier_2_tags', {}).keys():
                    if tag in vocab['tier_2_tags'][domain]:
                        vocab['tier_2_tags'][domain].remove(tag)

                print(f"✓ Removed: {removed_count} rules set to needs_tags, removed '{tag}' from vocabulary")
                decisions_made += 1

            elif choice == '2':
                print("✓ Kept tag")
            else:
                print("⊘ Skipped")

    # Save vocabulary if changes made
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

        print(f"\n✓ Vocabulary updated ({decisions_made} changes made)")
    else:
        print("\n✓ No changes made")

    print("\nReview complete.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
