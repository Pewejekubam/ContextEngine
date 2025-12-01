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


def check_untagged_count(db_path):
    """VOCAB-030: Check reports count of untagged rules."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    count = cursor.fetchone()[0]

    conn.close()
    return count


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
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def check_typos(vocab):
    """VOCAB-031: Check reports obvious typos (edit distance = 1)."""
    typos = []

    # Check within each domain
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        # Compare all pairs within domain
        for i, tag1 in enumerate(tags):
            for tag2 in tags[i+1:]:
                if levenshtein_distance(tag1, tag2) == 1:
                    typos.append({
                        'domain': domain,
                        'tag1': tag1,
                        'tag2': tag2
                    })

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

    if not tier1_valid or not tier2_valid:
        return False, issues, auto_fixed

    tier_1_domains = vocab.get('tier_1_domains', {})
    tier_2_tags = vocab.get('tier_2_tags', {})

    # VOCAB-033a: Phantom domain detection
    phantom_domains = [domain for domain in tier_2_tags.keys() if domain not in tier_1_domains]

    # VOCAB-033b: Missing tier_2_tags detection
    missing_tier2 = [domain for domain in tier_1_domains.keys() if domain not in tier_2_tags]

    # Auto-fix missing tier_2_tags
    if missing_tier2:
        for domain in missing_tier2:
            vocab['tier_2_tags'][domain] = []

        # Save vocabulary with auto-fix
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

    return phantom_domains, missing_tier2, auto_fixed


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

    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"
    db_path = BASE_DIR / "data" / "rules.db"

    if not vocab_path.exists():
        print(f"Error: Vocabulary file not found: {vocab_path}", file=sys.stderr)
        sys.exit(1)

    if not db_path.exists():
        print(f"Error: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # VOCAB-038: Get database statistics
    stats = get_database_statistics(db_path)

    # VOCAB-033: Schema validation
    phantom_domains, missing_tier2, auto_fixed = validate_vocabulary_schema(vocab, vocab_path)

    # VOCAB-030: Check untagged count
    untagged_count = check_untagged_count(db_path)

    # VOCAB-031: Check typos
    typos = check_typos(vocab)

    # VOCAB-037: Always print validation checklist
    print("\nSchema Validation:")
    tier1_valid = isinstance(vocab.get('tier_1_domains'), dict)
    tier2_valid = isinstance(vocab.get('tier_2_tags'), dict)

    print(f"  {'✓' if tier1_valid else '✗'} tier_1_domains is dict: {tier1_valid}")
    print(f"  {'✓' if tier2_valid else '✗'} tier_2_tags is dict: {tier2_valid}")
    print(f"  {'✓' if len(phantom_domains) == 0 else '✗'} No phantom domains: {len(phantom_domains) == 0}")

    # Show auto-fix message if applicable
    if auto_fixed:
        print(f"  ✓ All domains have tier_2_tags entry: True (auto-fixed {len(missing_tier2)} domain(s))")
    else:
        print(f"  ✓ All domains have tier_2_tags entry: {len(missing_tier2) == 0}")

    print("\nDatabase Statistics:")
    print(f"  Total rules: {stats['total_rules']}")
    print(f"  Untagged rules: {untagged_count}")
    print(f"  Typos detected: {len(typos)}")
    print()

    # VOCAB-033a: Report phantom domains (error)
    if phantom_domains:
        print(f"❌ ERROR: Phantom domains in tier_2_tags: {', '.join(phantom_domains)}")
        print("   Action: Manually remove phantom entries from config/tag-vocabulary.yaml")
        sys.exit(1)

    # VOCAB-033b: Report auto-fixed missing tier_2_tags (warning)
    if auto_fixed:
        print(f"⚠️  WARNING: Missing tier_2_tags for: {', '.join(missing_tier2)}")
        print("   Auto-fixed: Created empty tier_2_tags entries")

    # VOCAB-032 & VOCAB-034: Exit codes
    if untagged_count > 0:
        print(f"⚠️  WARNING: {untagged_count} rules need tags")
        sys.exit(1)

    if len(typos) > 0:
        print(f"⚠️  WARNING: {len(typos)} potential typos detected")
        for typo in typos[:5]:  # Show first 5
            print(f"   - {typo['domain']}: '{typo['tag1']}' vs '{typo['tag2']}'")
        if len(typos) > 5:
            print(f"   ... and {len(typos) - 5} more")
        sys.exit(1)

    print("✓ Vocabulary healthy")
    return 0


if __name__ == '__main__':
    sys.exit(main())
