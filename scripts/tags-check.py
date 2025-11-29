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


def get_database_statistics(db_path):
    """VOCAB-038: Query database statistics on startup."""
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
    """Calculate edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost of insertions, deletions, or substitutions
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
        # Compare all tags within domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                distance = levenshtein_distance(tag1, tag2)
                if distance == 1:
                    typos.append((domain, tag1, tag2))

    return typos


def validate_vocabulary_schema(vocab, vocab_path):
    """VOCAB-033: Validate vocabulary schema structure for tier_1/tier_2 consistency."""
    issues = []
    auto_fixed = False

    # Check tier_1_domains is dict
    tier1_valid = isinstance(vocab.get('tier_1_domains'), dict)
    if not tier1_valid:
        issues.append("tier_1_domains is not a dict")

    # Check tier_2_tags is dict
    tier2_valid = isinstance(vocab.get('tier_2_tags'), dict)
    if not tier2_valid:
        issues.append("tier_2_tags is not a dict")

    # VOCAB-033a: Check for phantom domains (tier_2_tags keys not in tier_1_domains)
    phantom_domains = []
    if tier1_valid and tier2_valid:
        tier1_keys = set(vocab['tier_1_domains'].keys())
        tier2_keys = set(vocab['tier_2_tags'].keys())
        phantom_domains = list(tier2_keys - tier1_keys)

    # VOCAB-033b: Check for missing tier_2_tags entries (tier_1_domains without tier_2_tags entry)
    missing_tier2 = []
    if tier1_valid and tier2_valid:
        tier1_keys = set(vocab['tier_1_domains'].keys())
        tier2_keys = set(vocab['tier_2_tags'].keys())
        missing_tier2 = list(tier1_keys - tier2_keys)

        # Auto-fix missing tier_2_tags entries
        if missing_tier2:
            for domain in missing_tier2:
                vocab['tier_2_tags'][domain] = []
            # Save the updated vocabulary
            with open(vocab_path, 'w') as f:
                yaml.dump(
                    vocab,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2,
                    allow_unicode=True
                )
            auto_fixed = True

    return {
        'tier1_valid': tier1_valid,
        'tier2_valid': tier2_valid,
        'phantom_domains': phantom_domains,
        'missing_tier2': missing_tier2,
        'auto_fixed': auto_fixed,
        'issues': issues
    }


def count_untagged_rules(db_path):
    """VOCAB-030: Count untagged rules."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    count = cursor.fetchone()[0]

    conn.close()
    return count


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

    # Get paths from config
    db_path = BASE_DIR / "data" / "rules.db"
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"

    # VOCAB-038: Get database statistics
    stats = get_database_statistics(db_path)

    # Load vocabulary
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # VOCAB-033: Validate vocabulary schema
    schema_result = validate_vocabulary_schema(vocab, vocab_path)

    # VOCAB-037: Print schema validation results
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_result['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_result['tier1_valid']}")
    print(f"  {'✓' if schema_result['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_result['tier2_valid']}")
    print(f"  {'✓' if not schema_result['phantom_domains'] else '✗'} No phantom domains: {not bool(schema_result['phantom_domains'])}")
    print(f"  {'✓' if not schema_result['missing_tier2'] or schema_result['auto_fixed'] else '✗'} All domains have tier_2_tags entry: {not bool(schema_result['missing_tier2']) or schema_result['auto_fixed']}")

    if schema_result['auto_fixed']:
        print(f"\n  Auto-fixed missing tier_2_tags for: {', '.join(schema_result['missing_tier2'])}")

    # VOCAB-030: Count untagged rules
    untagged_count = count_untagged_rules(db_path)

    # VOCAB-031: Detect typos
    typos = detect_typos(vocab)

    # VOCAB-037: Print database statistics
    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {untagged_count}")
    print(f"  Typos detected: {len(typos)}")

    # VOCAB-034: Determine exit code and status message
    status_message = ""
    exit_code = 0

    # VOCAB-033a: Check for phantom domains (highest priority)
    if schema_result['phantom_domains']:
        status_message = f"❌ FAILED: Phantom domains found: {', '.join(schema_result['phantom_domains'])}"
        exit_code = 1
    # VOCAB-032: Check for untagged rules
    elif untagged_count > 0:
        status_message = f"⚠️  WARNING: {untagged_count} rules need tags"
        exit_code = 1
    # VOCAB-032: Check for typos
    elif len(typos) > 0:
        status_message = f"⚠️  WARNING: {len(typos)} potential typos detected"
        exit_code = 1
    else:
        status_message = "✓ Vocabulary healthy"
        exit_code = 0

    print(f"\n{status_message}")

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
