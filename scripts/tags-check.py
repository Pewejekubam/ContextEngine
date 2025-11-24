#!/usr/bin/env python3
"""
Pre-commit vocabulary health check (untagged count, typo detection, schema validation, exit codes)

Implements constraints: VOCAB-030 through VOCAB-034, VOCAB-037, VOCAB-038
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
    if not db_path.exists():
        return {
            'total_rules': 0,
            'tagged_rules': 0,
            'unique_tags': 0
        }

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

    Used for VOCAB-031 typo detection (edit distance = 1).
    """
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


def detect_typos(vocab):
    """
    Detect typos using edit distance = 1 (VOCAB-031).

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


def check_untagged_count(db_path):
    """
    Count untagged rules (VOCAB-030).

    Returns:
        int: Count of rules with tags_state = 'needs_tags'
    """
    if not db_path.exists():
        return 0

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    count = cursor.fetchone()[0]

    conn.close()
    return count


def validate_vocabulary_schema(vocab, vocab_path):
    """
    Validate vocabulary schema for tier_1/tier_2 consistency (VOCAB-033).

    Returns:
        dict with validation results
    """
    results = {
        'tier1_valid': False,
        'tier2_valid': False,
        'no_phantoms': False,
        'all_have_entry': False,
        'phantom_domains': [],
        'missing_tier2': [],
        'auto_fixed': False
    }

    # Check tier_1_domains is dict
    tier1 = vocab.get('tier_1_domains')
    results['tier1_valid'] = isinstance(tier1, dict)

    # Check tier_2_tags is dict
    tier2 = vocab.get('tier_2_tags')
    results['tier2_valid'] = isinstance(tier2, dict)

    if not results['tier1_valid'] or not results['tier2_valid']:
        return results

    # VOCAB-033a: Check for phantom domains (tier_2_tags keys not in tier_1_domains)
    tier1_keys = set(tier1.keys())
    tier2_keys = set(tier2.keys())

    phantom_domains = tier2_keys - tier1_keys
    results['phantom_domains'] = sorted(phantom_domains)
    results['no_phantoms'] = len(phantom_domains) == 0

    # VOCAB-033b: Check for missing tier_2_tags entries (tier_1_domains without tier_2_tags)
    missing_tier2 = tier1_keys - tier2_keys
    results['missing_tier2'] = sorted(missing_tier2)
    results['all_have_entry'] = len(missing_tier2) == 0

    # Auto-fix missing tier_2_tags entries
    if len(missing_tier2) > 0:
        for domain in missing_tier2:
            vocab['tier_2_tags'][domain] = []

        # Save updated vocabulary
        with open(vocab_path, 'w') as f:
            yaml.dump(
                vocab,
                f,
                default_flow_style=False,
                sort_keys=False,
                indent=2,
                allow_unicode=True
            )

        results['auto_fixed'] = True
        results['all_have_entry'] = True

    return results


def main():
    """Pre-commit vocabulary health check"""
    print("Vocabulary Health Check")
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

    # Load vocabulary
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-038: Get database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-033: Validate vocabulary schema
    schema_validation = validate_vocabulary_schema(vocab, vocab_path)

    # VOCAB-030: Check untagged count
    untagged_count = check_untagged_count(db_path)

    # VOCAB-031: Detect typos
    typos = detect_typos(vocab)
    typo_count = len(typos)

    # VOCAB-037: Report schema validation results explicitly
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_validation['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_validation['tier1_valid']}")
    print(f"  {'✓' if schema_validation['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_validation['tier2_valid']}")
    print(f"  {'✓' if schema_validation['no_phantoms'] else '✗'} No phantom domains: {schema_validation['no_phantoms']}")
    print(f"  {'✓' if schema_validation['all_have_entry'] else '✗'} All domains have tier_2_tags entry: {schema_validation['all_have_entry']}")

    if schema_validation['auto_fixed']:
        print(f"\n  ⚠️  Auto-fixed missing tier_2_tags for: {', '.join(schema_validation['missing_tier2'])}")

    print(f"\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {untagged_count}")
    print(f"  Typos detected: {typo_count}")

    # Show typos if any
    if typo_count > 0:
        print(f"\nPotential typos:")
        for tag1, tag2, domain in typos[:10]:  # Show max 10
            print(f"  - {domain}: '{tag1}' vs '{tag2}'")
        if typo_count > 10:
            print(f"  ... and {typo_count - 10} more")

    # Determine status and exit code
    has_issues = False
    status_messages = []

    # VOCAB-034: Exit code 1 if phantom domains detected
    if len(schema_validation['phantom_domains']) > 0:
        status_messages.append(f"❌ FAILED: Phantom domains found: {', '.join(schema_validation['phantom_domains'])}")
        has_issues = True
    if untagged_count > 0:
        status_messages.append(f"⚠️  WARNING: {untagged_count} rules need tags")
        has_issues = True
    if typo_count > 0:
        status_messages.append(f"⚠️  WARNING: {typo_count} potential typos detected")
        has_issues = True

    if not has_issues:
        status_messages.append("✓ Vocabulary healthy")

    # Print status messages
    print("")
    for msg in status_messages:
        print(msg)

    # VOCAB-032: Exit 0 if healthy, 1 if issues found
    if has_issues:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
