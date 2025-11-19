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
    """Query database statistics."""
    if not db_path.exists():
        return {
            'total_rules': 0,
            'untagged_count': 0,
            'unique_tags': 0
        }

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Total rules
        cursor.execute("SELECT COUNT(*) FROM rules")
        total_rules = cursor.fetchone()[0]

        # Untagged rules (VOCAB-030)
        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
        untagged_count = cursor.fetchone()[0]

        # Unique tags
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
            'untagged_count': untagged_count,
            'unique_tags': unique_tags
        }
    except sqlite3.OperationalError:
        # Table doesn't exist
        return {
            'total_rules': 0,
            'untagged_count': 0,
            'unique_tags': 0
        }
    finally:
        conn.close()


def detect_typos(vocab):
    """VOCAB-031: Detect typos using edit distance = 1."""
    typos = []

    for domain, tags in vocab.get('tier_2_tags', {}).items():
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def validate_vocabulary_schema(vocab):
    """VOCAB-033: Validate vocabulary schema structure."""
    issues = {
        'tier1_valid': False,
        'tier2_valid': False,
        'phantom_domains': [],
        'missing_tier2': [],
        'all_have_entry': False,
        'no_phantoms': False
    }

    # Check tier_1_domains is dict
    tier1 = vocab.get('tier_1_domains', {})
    issues['tier1_valid'] = isinstance(tier1, dict)

    # Check tier_2_tags is dict
    tier2 = vocab.get('tier_2_tags', {})
    issues['tier2_valid'] = isinstance(tier2, dict)

    if not issues['tier1_valid'] or not issues['tier2_valid']:
        return issues

    # VOCAB-033a: Phantom domain detection
    tier1_keys = set(tier1.keys())
    tier2_keys = set(tier2.keys())

    issues['phantom_domains'] = sorted(tier2_keys - tier1_keys)
    issues['no_phantoms'] = len(issues['phantom_domains']) == 0

    # VOCAB-033b: Missing tier_2_tags detection
    issues['missing_tier2'] = sorted(tier1_keys - tier2_keys)
    issues['all_have_entry'] = len(issues['missing_tier2']) == 0

    return issues


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
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    db_path = BASE_DIR / "data" / "rules.db"

    # Load vocabulary
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # VOCAB-033: Validate schema structure
    schema_issues = validate_vocabulary_schema(vocab)

    # Get database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-031: Detect typos
    typos = detect_typos(vocab)

    # VOCAB-037: Report schema validation results explicitly
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_issues['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_issues['tier1_valid']}")
    print(f"  {'✓' if schema_issues['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_issues['tier2_valid']}")
    print(f"  {'✓' if schema_issues['no_phantoms'] else '✗'} No phantom domains: {schema_issues['no_phantoms']}")
    print(f"  {'✓' if schema_issues['all_have_entry'] else '✗'} All domains have tier_2_tags entry: {schema_issues['all_have_entry']}")

    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {stats['untagged_count']}")
    print(f"  Typos detected: {len(typos)}")

    # VOCAB-033b: Auto-fix missing tier_2_tags entries
    if len(schema_issues['missing_tier2']) > 0:
        print(f"\n⚠️  WARNING: Missing tier_2_tags for: {', '.join(schema_issues['missing_tier2'])}")
        print("Auto-fixing by creating empty entries...")

        for domain in schema_issues['missing_tier2']:
            vocab['tier_2_tags'][domain] = []

        # Save vocabulary
        with open(vocab_path, 'w') as f:
            yaml.dump(
                vocab,
                f,
                default_flow_style=False,
                sort_keys=False,
                indent=2,
                allow_unicode=True
            )
        print("✓ Auto-fix complete")

    # Determine exit code and status message
    has_errors = False
    status_messages = []

    # VOCAB-033a: Phantom domains (ERROR)
    if len(schema_issues['phantom_domains']) > 0:
        status_messages.append(f"❌ ERROR: Phantom domains in tier_2_tags: {', '.join(schema_issues['phantom_domains'])}")
        status_messages.append("   Manually remove phantom entries from config/tag-vocabulary.yaml")
        has_errors = True

    # VOCAB-030: Untagged rules (WARNING)
    if stats['untagged_count'] > 0:
        status_messages.append(f"⚠️  WARNING: {stats['untagged_count']} rules need tags")
        has_errors = True

    # VOCAB-031: Typos (WARNING)
    if len(typos) > 0:
        status_messages.append(f"⚠️  WARNING: {len(typos)} potential typos detected")
        for domain, tag1, tag2 in typos[:3]:  # Show first 3
            status_messages.append(f"   - {domain}: '{tag1}' vs '{tag2}'")
        if len(typos) > 3:
            status_messages.append(f"   ... and {len(typos) - 3} more")
        has_errors = True

    # Print status
    print()
    if has_errors:
        for msg in status_messages:
            print(msg)
        sys.exit(1)
    else:
        print("✓ Vocabulary healthy")
        sys.exit(0)


if __name__ == '__main__':
    main()
