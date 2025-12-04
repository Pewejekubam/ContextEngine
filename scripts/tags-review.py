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
from datetime import datetime


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein distance between two strings."""
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
    """VOCAB-038: Get database statistics (total rules, tagged rules, unique tags)"""
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


def detect_typos(vocab_path):
    """VOCAB-020: Detect typos using edit distance = 1 within each domain"""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    typo_pairs = []
    tier_2_tags = vocab.get('tier_2_tags', {})

    for domain, tags in tier_2_tags.items():
        # Compare all pairs within domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typo_pairs.append({
                        'domain': domain,
                        'tag1': tag1,
                        'tag2': tag2
                    })

    return typo_pairs


def detect_rare_tags(db_path):
    """VOCAB-022: Detect rare tags (1-2 uses across all rules in database)"""
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

    rare_tags = [{'tag': row[0], 'count': row[1]} for row in cursor.fetchall()]
    conn.close()

    return rare_tags


def update_rule_tags(db_path, rule_id, new_tags):
    """VOCAB-023: Update affected rules when merging synonyms"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE rules SET tags = ?, curated_at = ? WHERE id = ?",
        (json.dumps(new_tags), datetime.utcnow().isoformat() + 'Z', rule_id)
    )

    conn.commit()
    conn.close()


def set_needs_tags(db_path, rule_id):
    """VOCAB-024: Set tags_state='needs_tags' for rules with removed tags"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE rules SET tags_state = 'needs_tags' WHERE id = ?",
        (rule_id,)
    )

    conn.commit()
    conn.close()


def get_rules_with_tag(db_path, tag):
    """Get all rules using a specific tag"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT r.id, r.title, r.tags
        FROM rules r, json_each(r.tags)
        WHERE json_each.value = ?
    """, (tag,))

    rules = []
    for row in cursor.fetchall():
        rules.append({
            'id': row[0],
            'title': row[1],
            'tags': json.loads(row[2])
        })

    conn.close()
    return rules


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
    db_path = BASE_DIR / 'data' / 'rules.db'
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'

    # VOCAB-038: Get database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-019: Query current vocabulary state from filesystem
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # Detect issues
    typo_pairs = detect_typos(vocab_path)
    rare_tags = detect_rare_tags(db_path)

    # VOCAB-036: Empty database state reporting
    if len(typo_pairs) == 0 and len(rare_tags) == 0:
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

    # Add typo issues
    for pair in typo_pairs[:5]:
        issues.append({
            'type': 'typo',
            'data': pair
        })

    # Add rare tag issues (fill up to 5 total)
    for rare in rare_tags[:max(0, 5 - len(issues))]:
        issues.append({
            'type': 'rare',
            'data': rare
        })

    # Interactive review
    decisions_made = 0
    for i, issue in enumerate(issues):
        if decisions_made >= 5:
            break

        print(f"\n[{i+1}/{len(issues)}]")

        if issue['type'] == 'typo':
            pair = issue['data']
            print(f"Potential typo in domain '{pair['domain']}':")
            print(f"  Tag 1: {pair['tag1']}")
            print(f"  Tag 2: {pair['tag2']}")
            print("\nActions:")
            print("  1) Keep both (not a typo)")
            print("  2) Merge tag2 → tag1")
            print("  3) Merge tag1 → tag2")
            print("  4) Skip")

            choice = input("\nChoice: ").strip()

            if choice == '1':
                print("Kept both tags.")
                decisions_made += 1
            elif choice == '2':
                # Merge tag2 into tag1
                rules = get_rules_with_tag(db_path, pair['tag2'])
                for rule in rules:
                    new_tags = [t if t != pair['tag2'] else pair['tag1'] for t in rule['tags']]
                    new_tags = list(dict.fromkeys(new_tags))  # Remove duplicates
                    update_rule_tags(db_path, rule['id'], new_tags)

                # Remove tag2 from vocabulary
                vocab['tier_2_tags'][pair['domain']].remove(pair['tag2'])
                with open(vocab_path, 'w') as f:
                    yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2)

                print(f"Merged {len(rules)} rules: {pair['tag2']} → {pair['tag1']}")
                decisions_made += 1
            elif choice == '3':
                # Merge tag1 into tag2
                rules = get_rules_with_tag(db_path, pair['tag1'])
                for rule in rules:
                    new_tags = [t if t != pair['tag1'] else pair['tag2'] for t in rule['tags']]
                    new_tags = list(dict.fromkeys(new_tags))  # Remove duplicates
                    update_rule_tags(db_path, rule['id'], new_tags)

                # Remove tag1 from vocabulary
                vocab['tier_2_tags'][pair['domain']].remove(pair['tag1'])
                with open(vocab_path, 'w') as f:
                    yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2)

                print(f"Merged {len(rules)} rules: {pair['tag1']} → {pair['tag2']}")
                decisions_made += 1
            else:
                print("Skipped.")

        elif issue['type'] == 'rare':
            rare = issue['data']
            rules = get_rules_with_tag(db_path, rare['tag'])
            print(f"Rare tag: '{rare['tag']}' (used {rare['count']} time(s))")
            print(f"\nUsed in {len(rules)} rule(s):")
            for rule in rules:
                print(f"  - {rule['id']}: {rule['title']}")
            print("\nActions:")
            print("  1) Keep tag")
            print("  2) Remove tag (mark rules as needs_tags)")
            print("  3) Skip")

            choice = input("\nChoice: ").strip()

            if choice == '1':
                print("Kept tag.")
                decisions_made += 1
            elif choice == '2':
                # Remove tag from all rules
                for rule in rules:
                    new_tags = [t for t in rule['tags'] if t != rare['tag']]
                    if len(new_tags) == 0:
                        # VOCAB-024: Set needs_tags if no tags remain
                        set_needs_tags(db_path, rule['id'])
                        update_rule_tags(db_path, rule['id'], [])
                    else:
                        update_rule_tags(db_path, rule['id'], new_tags)

                # Find domain and remove from vocabulary
                for domain, tags in vocab['tier_2_tags'].items():
                    if rare['tag'] in tags:
                        tags.remove(rare['tag'])
                        break

                with open(vocab_path, 'w') as f:
                    yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2)

                print(f"Removed tag from {len(rules)} rules.")
                decisions_made += 1
            else:
                print("Skipped.")

    print(f"\nReview complete. {decisions_made} decision(s) made.")

    if len(issues) > 5:
        print(f"\n{len(issues) - 5} more issue(s) remain. Re-run to continue review.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
