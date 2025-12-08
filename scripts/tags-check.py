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


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein edit distance between two strings (VOCAB-031)."""
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


def get_database_statistics(db_path):
    """Query database statistics (VOCAB-038)."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Total rules
    cursor.execute("SELECT COUNT(*) FROM rules")
    total_rules = cursor.fetchone()[0]

    # Untagged rules (VOCAB-030)
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    untagged_count = cursor.fetchone()[0]

    conn.close()

    return {
        'total_rules': total_rules,
        'untagged_count': untagged_count
    }


def validate_vocabulary_schema(vocabulary):
    """Validate vocabulary schema structure (VOCAB-033)."""
    issues = {
        'tier1_valid': False,
        'tier2_valid': False,
        'phantom_domains': [],
        'missing_tier2': []
    }

    # Check tier_1_domains is dict
    tier_1_domains = vocabulary.get('tier_1_domains', {})
    if isinstance(tier_1_domains, dict):
        issues['tier1_valid'] = True

    # Check tier_2_tags is dict
    tier_2_tags = vocabulary.get('tier_2_tags', {})
    if isinstance(tier_2_tags, dict):
        issues['tier2_valid'] = True

    # VOCAB-033a: Check for phantom domains (tier_2_tags keys not in tier_1_domains)
    for domain in tier_2_tags.keys():
        if domain not in tier_1_domains:
            issues['phantom_domains'].append(domain)

    # VOCAB-033b: Check for missing tier_2_tags entries (tier_1_domains without tier_2_tags entry)
    for domain in tier_1_domains.keys():
        if domain not in tier_2_tags:
            issues['missing_tier2'].append(domain)

    return issues


def detect_typos(vocabulary):
    """Detect typos using edit distance = 1 (VOCAB-031)."""
    typo_count = 0
    tier_2_tags = vocabulary.get('tier_2_tags', {})

    for domain, tags in tier_2_tags.items():
        # Compare all tags within each domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typo_count += 1

    return typo_count


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

    # Load vocabulary
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    try:
        with open(vocab_path) as f:
            vocabulary = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path
    db_path = BASE_DIR / "data" / "rules.db"

    # VOCAB-033: Validate vocabulary schema
    schema_issues = validate_vocabulary_schema(vocabulary)

    # VOCAB-038: Query database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-031: Detect typos
    typo_count = detect_typos(vocabulary)

    # VOCAB-037: Report schema validation results explicitly
    print("\nSchema Validation:")
    print(f"  {'✓' if schema_issues['tier1_valid'] else '✗'} tier_1_domains is dict: {schema_issues['tier1_valid']}")
    print(f"  {'✓' if schema_issues['tier2_valid'] else '✗'} tier_2_tags is dict: {schema_issues['tier2_valid']}")
    print(f"  {'✓' if len(schema_issues['phantom_domains']) == 0 else '✗'} No phantom domains: {len(schema_issues['phantom_domains']) == 0}")
    print(f"  {'✓' if len(schema_issues['missing_tier2']) == 0 else '✗'} All domains have tier_2_tags entry: {len(schema_issues['missing_tier2']) == 0}")

    # VOCAB-033b: Auto-fix missing tier_2_tags entries
    if len(schema_issues['missing_tier2']) > 0:
        print(f"\n⚠️  WARNING: Missing tier_2_tags for: {', '.join(schema_issues['missing_tier2'])}")
        print("Auto-fixing: Creating empty tier_2_tags entries...")

        # Auto-fix by creating empty entries
        tier_2_tags = vocabulary.get('tier_2_tags', {})
        for domain in schema_issues['missing_tier2']:
            tier_2_tags[domain] = []

        # Save updated vocabulary
        try:
            with open(vocab_path, 'w') as f:
                yaml.dump(
                    vocabulary,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2,
                    allow_unicode=True
                )
            print("✓ Auto-fix complete. Empty tier_2_tags entries created.")
        except Exception as e:
            print(f"Error saving vocabulary: {e}", file=sys.stderr)

    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {stats['untagged_count']}")
    print(f"  Typos detected: {typo_count}")

    # VOCAB-034: Determine exit code and status message
    exit_code = 0
    status_messages = []

    # VOCAB-033a: Phantom domains (error)
    if len(schema_issues['phantom_domains']) > 0:
        status_messages.append(f"❌ FAILED: Phantom domains found: {', '.join(schema_issues['phantom_domains'])}")
        exit_code = 1

    # VOCAB-030: Untagged rules (warning)
    if stats['untagged_count'] > 0:
        status_messages.append(f"⚠️  WARNING: {stats['untagged_count']} rules need tags")
        exit_code = 1

    # VOCAB-031: Typos (warning)
    if typo_count > 0:
        status_messages.append(f"⚠️  WARNING: {typo_count} potential typos detected")
        exit_code = 1

    # VOCAB-032: Healthy status
    if exit_code == 0:
        status_messages.append("✓ Vocabulary healthy")

    # Print status messages
    print()
    for msg in status_messages:
        print(msg)

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
