#!/usr/bin/env python3
"""
Pre-commit vocabulary health check (untagged count, typo detection, schema validation, exit codes)

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


def check_untagged_count(db_path):
    """VOCAB-030: Check reports count of untagged rules"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    count = cursor.fetchone()[0]

    conn.close()
    return count


def detect_typos(vocab_path):
    """VOCAB-031: Check reports obvious typos (edit distance = 1)"""
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


def validate_vocabulary_schema(vocab_path):
    """VOCAB-033: Validate vocabulary schema structure for tier_1/tier_2 consistency"""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    issues = {
        'tier1_valid': False,
        'tier2_valid': False,
        'phantom_domains': [],
        'missing_tier2': [],
        'all_have_entry': False
    }

    # Check tier_1_domains is dict
    tier_1_domains = vocab.get('tier_1_domains', {})
    issues['tier1_valid'] = isinstance(tier_1_domains, dict)

    # Check tier_2_tags is dict
    tier_2_tags = vocab.get('tier_2_tags', {})
    issues['tier2_valid'] = isinstance(tier_2_tags, dict)

    if not issues['tier1_valid'] or not issues['tier2_valid']:
        return issues, False

    # VOCAB-033a: Phantom domain detection
    for domain in tier_2_tags.keys():
        if domain not in tier_1_domains:
            issues['phantom_domains'].append(domain)

    # VOCAB-033b: Missing tier_2_tags detection
    for domain in tier_1_domains.keys():
        if domain not in tier_2_tags:
            issues['missing_tier2'].append(domain)

    # Check if all domains have tier_2_tags entry (after auto-fix)
    issues['all_have_entry'] = len(issues['missing_tier2']) == 0

    # Auto-fix missing tier_2_tags entries
    if len(issues['missing_tier2']) > 0:
        for domain in issues['missing_tier2']:
            tier_2_tags[domain] = []

        # Save updated vocabulary
        with open(vocab_path, 'w') as f:
            yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2)

        # Update status after fix
        issues['all_have_entry'] = True

    # Schema is valid if no phantom domains
    schema_valid = len(issues['phantom_domains']) == 0

    return issues, schema_valid


def main():
    """Pre-commit vocabulary health check (untagged count, typo detection, schema validation, exit codes)"""
    print("Vocabulary Health Check")
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

    # VOCAB-033: Validate vocabulary schema
    try:
        schema_issues, schema_valid = validate_vocabulary_schema(vocab_path)
    except Exception as e:
        print(f"Error validating vocabulary schema: {e}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-030: Check untagged count
    untagged_count = check_untagged_count(db_path)

    # VOCAB-031: Check typos
    typo_pairs = detect_typos(vocab_path)
    typo_count = len(typo_pairs)

    # VOCAB-037: Report schema validation results explicitly
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_issues['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_issues['tier1_valid']}")
    print(f"  {'✓' if schema_issues['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_issues['tier2_valid']}")
    print(f"  {'✓' if len(schema_issues['phantom_domains']) == 0 else '✗'} No phantom domains: {len(schema_issues['phantom_domains']) == 0}")
    print(f"  {'✓' if schema_issues['all_have_entry'] else '✗'} All domains have tier_2_tags entry: {schema_issues['all_have_entry']}")

    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {untagged_count}")
    print(f"  Typos detected: {typo_count}")

    # VOCAB-033b: Report missing tier_2_tags (if auto-fixed)
    if len(schema_issues['missing_tier2']) > 0:
        print(f"\n⚠️  WARNING: Missing tier_2_tags for: {', '.join(schema_issues['missing_tier2'])}")
        print("   (Auto-fixed: Created empty tier_2_tags entries)")

    # VOCAB-033a: Report phantom domains
    if len(schema_issues['phantom_domains']) > 0:
        print(f"\n❌ ERROR: Phantom domains in tier_2_tags: {', '.join(schema_issues['phantom_domains'])}")
        print("   Action: Manually remove phantom entries from config/tag-vocabulary.yaml")

    # Determine status and exit code
    if len(schema_issues['phantom_domains']) > 0:
        status_message = f"❌ FAILED: Phantom domains found: {', '.join(schema_issues['phantom_domains'])}"
        exit_code = 1
    elif untagged_count > 0:
        status_message = f"⚠️  WARNING: {untagged_count} rules need tags"
        exit_code = 1
    elif typo_count > 0:
        status_message = f"⚠️  WARNING: {typo_count} potential typos detected"
        exit_code = 1
    else:
        status_message = "✓ Vocabulary healthy"
        exit_code = 0

    print(f"\n{status_message}")

    # Show typo details if any
    if typo_count > 0:
        print("\nPotential typos:")
        for i, pair in enumerate(typo_pairs[:10]):  # Show first 10
            print(f"  {i+1}. Domain '{pair['domain']}': {pair['tag1']} / {pair['tag2']}")
        if typo_count > 10:
            print(f"  ... and {typo_count - 10} more")

    # VOCAB-034: Exit with code 1 if issues found
    sys.exit(exit_code)


if __name__ == '__main__':
    sys.exit(main())
