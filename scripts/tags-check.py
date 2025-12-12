#!/usr/bin/env python3
"""
Pre-commit vocabulary health check (untagged count, typo detection, schema validation, exit codes)

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


def check_untagged_count(db_path):
    """VOCAB-030: Check reports count of untagged rules."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    count = cursor.fetchone()[0]

    conn.close()
    return count


def check_typos(vocab_path):
    """VOCAB-031: Check reports obvious typos (edit distance = 1)."""
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


def validate_vocabulary_schema(vocab_path):
    """VOCAB-033: tags-check validates vocabulary schema structure for tier_1/tier_2 consistency."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    tier_1_domains = vocab.get('tier_1_domains', {})
    tier_2_tags = vocab.get('tier_2_tags', {})

    # Check types
    tier1_valid = isinstance(tier_1_domains, dict)
    tier2_valid = isinstance(tier_2_tags, dict)

    # VOCAB-033a: Phantom domain detection
    phantom_domains = []
    if tier2_valid and tier1_valid:
        phantom_domains = [domain for domain in tier_2_tags.keys() if domain not in tier_1_domains]

    # VOCAB-033b: Missing tier_2_tags detection
    missing_tier2 = []
    if tier1_valid and tier2_valid:
        missing_tier2 = [domain for domain in tier_1_domains.keys() if domain not in tier_2_tags]

    # Auto-fix missing tier_2_tags entries
    if missing_tier2:
        for domain in missing_tier2:
            tier_2_tags[domain] = []

        # Save updated vocabulary
        with open(vocab_path, 'w') as f:
            yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)

    return {
        'tier1_valid': tier1_valid,
        'tier2_valid': tier2_valid,
        'phantom_domains': phantom_domains,
        'missing_tier2': missing_tier2,
        'no_phantoms': len(phantom_domains) == 0,
        'all_have_entry': len(missing_tier2) == 0 or True  # True after auto-fix
    }


def main():
    """Vocabulary curation workflows: typo detection, synonym merging, rare tag cleanup, and pre-commit health checks"""
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
    db_path = Path(config['structure']['database_path'])

    # Validate schema (VOCAB-033)
    schema_validation = validate_vocabulary_schema(vocab_path)

    # Get database statistics (VOCAB-038)
    stats = get_database_statistics(db_path)

    # Check for issues
    untagged_count = check_untagged_count(db_path)
    typos = check_typos(vocab_path)

    # VOCAB-037: Report schema validation results explicitly
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_validation['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_validation['tier1_valid']}")
    print(f"  {'✓' if schema_validation['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_validation['tier2_valid']}")
    print(f"  {'✓' if schema_validation['no_phantoms'] else '✗'} No phantom domains: {schema_validation['no_phantoms']}")
    print(f"  {'✓' if schema_validation['all_have_entry'] else '✗'} All domains have tier_2_tags entry: {schema_validation['all_have_entry']}")

    # Database statistics
    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {untagged_count}")
    print(f"  Typos detected: {len(typos)}\n")

    # Determine exit code and status message
    exit_code = 0
    status_messages = []

    # VOCAB-033a: Phantom domains
    if schema_validation['phantom_domains']:
        phantom_list = ', '.join(schema_validation['phantom_domains'])
        status_messages.append(f"ERROR: Phantom domains in tier_2_tags: {phantom_list}")
        exit_code = 1

    # VOCAB-033b: Missing tier_2_tags (auto-fixed)
    if schema_validation['missing_tier2']:
        missing_list = ', '.join(schema_validation['missing_tier2'])
        status_messages.append(f"WARNING: Missing tier_2_tags for: {missing_list} (auto-fixed)")

    # VOCAB-030: Untagged rules
    if untagged_count > 0:
        status_messages.append(f"WARNING: {untagged_count} rules need tags")
        exit_code = 1

    # VOCAB-031: Typos
    if len(typos) > 0:
        status_messages.append(f"WARNING: {len(typos)} potential typos detected")
        exit_code = 1

    # Print status
    if exit_code == 0:
        print("✓ Vocabulary healthy")
    else:
        for msg in status_messages:
            if "ERROR" in msg:
                print(f"❌ {msg}")
            else:
                print(f"⚠️  {msg}")

    # VOCAB-032, VOCAB-034: Exit codes
    sys.exit(exit_code)


if __name__ == '__main__':
    sys.exit(main())
