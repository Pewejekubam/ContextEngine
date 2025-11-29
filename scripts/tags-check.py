#!/usr/bin/env python3
"""
Pre-commit vocabulary health check (untagged count, typo detection, schema validation, exit codes)

Implements constraints: VOCAB-030, VOCAB-031, VOCAB-032, VOCAB-033, VOCAB-033a, VOCAB-033b, VOCAB-034, VOCAB-037, VOCAB-038
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
    """VOCAB-031: Calculate Levenshtein edit distance between two strings."""
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
    """VOCAB-031: Detect typos using edit distance = 1."""
    typos = []

    for domain, tags in vocab.get('tier_2_tags', {}).items():
        # Compare all pairs within domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def validate_vocabulary_schema(vocab):
    """VOCAB-033: Validate vocabulary schema structure for tier_1/tier_2 consistency."""
    issues = {
        'tier1_valid': True,
        'tier2_valid': True,
        'phantom_domains': [],
        'missing_tier2': [],
        'auto_fixed': False
    }

    # Check tier_1_domains is dict
    if not isinstance(vocab.get('tier_1_domains'), dict):
        issues['tier1_valid'] = False
        return issues

    # Check tier_2_tags is dict
    if not isinstance(vocab.get('tier_2_tags'), dict):
        issues['tier2_valid'] = False
        return issues

    tier_1_domains = set(vocab['tier_1_domains'].keys())
    tier_2_domains = set(vocab['tier_2_tags'].keys())

    # VOCAB-033a: Phantom domain detection
    phantom_domains = tier_2_domains - tier_1_domains
    if phantom_domains:
        issues['phantom_domains'] = sorted(phantom_domains)

    # VOCAB-033b: Missing tier_2_tags detection
    missing_tier2 = tier_1_domains - tier_2_domains
    if missing_tier2:
        issues['missing_tier2'] = sorted(missing_tier2)

    return issues


def auto_fix_missing_tier2(vocab, missing_domains, vocab_path):
    """VOCAB-033b: Auto-fix missing tier_2_tags entries."""
    for domain in missing_domains:
        vocab['tier_2_tags'][domain] = []

    # Save vocabulary with proper formatting (VOCAB-012)
    with open(vocab_path, 'w') as f:
        yaml.dump(
            vocab,
            f,
            default_flow_style=False,   # Block style
            sort_keys=False,             # Preserve insertion order
            indent=2,                    # 2-space indent
            allow_unicode=True           # Support international characters
        )


def check_untagged_count(db_path):
    """VOCAB-030: Check count of untagged rules."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    untagged_count = cursor.fetchone()[0]

    conn.close()
    return untagged_count


def main():
    """Pre-commit vocabulary health check (untagged count, typo detection, schema validation, exit codes)."""
    print("Vocabulary Health Check")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Load vocabulary
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path
    db_path = BASE_DIR / "data" / "rules.db"

    # VOCAB-033: Validate vocabulary schema
    schema_issues = validate_vocabulary_schema(vocab)

    # VOCAB-037: Report schema validation results explicitly
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_issues['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_issues['tier1_valid']}")
    print(f"  {'✓' if schema_issues['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_issues['tier2_valid']}")
    print(f"  {'✓' if not schema_issues['phantom_domains'] else '✗'} No phantom domains: {not schema_issues['phantom_domains']}")
    print(f"  {'✓' if not schema_issues['missing_tier2'] else '✗'} All domains have tier_2_tags entry: {not schema_issues['missing_tier2']}")

    # VOCAB-033b: Auto-fix missing tier_2_tags
    if schema_issues['missing_tier2']:
        print(f"\n⚠️  WARNING: Missing tier_2_tags for: {', '.join(schema_issues['missing_tier2'])}")
        print("    Auto-fixing by creating empty tier_2_tags entries...")
        auto_fix_missing_tier2(vocab, schema_issues['missing_tier2'], vocab_path)
        schema_issues['auto_fixed'] = True
        print("    ✓ Auto-fix complete.")

    # VOCAB-038: Get database statistics
    try:
        stats = get_database_statistics(db_path)
    except Exception as e:
        print(f"\nError reading database: {e}", file=sys.stderr)
        stats = {'total_rules': 0, 'tagged_rules': 0, 'unique_tags': 0}

    # VOCAB-030: Check untagged count
    try:
        untagged_count = check_untagged_count(db_path)
    except Exception as e:
        print(f"Error checking untagged count: {e}", file=sys.stderr)
        untagged_count = 0

    # VOCAB-031: Detect typos
    typos = detect_typos(vocab)
    typo_count = len(typos)

    # VOCAB-037: Report database statistics
    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {untagged_count}")
    print(f"  Typos detected: {typo_count}")

    # VOCAB-037: Status message
    print()
    has_issues = False

    # VOCAB-033a: Report phantom domains (error)
    if schema_issues['phantom_domains']:
        print(f"❌ ERROR: Phantom domains in tier_2_tags: {', '.join(schema_issues['phantom_domains'])}")
        print("   Manually remove phantom entries from config/tag-vocabulary.yaml")
        has_issues = True

    # Report untagged rules (warning)
    if untagged_count > 0:
        print(f"⚠️  WARNING: {untagged_count} rules need tags")
        has_issues = True

    # Report typos (warning)
    if typo_count > 0:
        print(f"⚠️  WARNING: {typo_count} potential typos detected")
        has_issues = True

    # VOCAB-032, VOCAB-034: Exit code based on issues
    if not has_issues:
        print("✓ Vocabulary healthy")
        return 0
    else:
        return 1


if __name__ == '__main__':
    sys.exit(main())
