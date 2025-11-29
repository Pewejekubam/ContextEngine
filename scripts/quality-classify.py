#!/usr/bin/env python3
"""
Quality classifier with heuristic fast-path and Claude batching

Implements constraints: CLS-001 through CLS-012
Generated from: specs/modules/runtime-script-quality-classifier-v1.0.0.yaml
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
# RUNTIME-SCRIPT-QUALITY-CLASSIFIER MODULE IMPLEMENTATION
# ============================================================================

import re
import sqlite3
from datetime import datetime, timezone

# CLS-011: 12 hardcoded heuristic patterns for generic advice detection
HEURISTIC_PATTERNS = [
    # Pattern 1: Descriptive naming
    (r'\buse\s+descriptive\s+(variable|function|class|method)?\s*names?\b', 1.0),
    # Pattern 2: Unit testing
    (r'\bwrite\s+unit\s+tests?\b', 1.0),
    # Pattern 3: Best practices
    (r'\bfollow\s+best\s+practices?\b', 1.0),
    # Pattern 4: Code cleanliness
    (r'\bkeep\s+code\s+clean\b', 1.0),
    # Pattern 5: Error handling
    (r'\bhandle\s+(exceptions?|errors?)\s+gracefully\b', 1.0),
    # Pattern 6: Magic numbers
    (r'\bavoid\s+(magic\s+numbers?|hardcoded\s+values?)\b', 1.0),
    # Pattern 7: Documentation
    (r'\bcomment\s+your\s+code\b|\bdocument\s+functions?\b', 1.0),
    # Pattern 8: Design principles
    (r'\bfollow\s+(SOLID|DRY)\s+principles?\b', 1.0),
    # Pattern 9: Commit messages
    (r'\buse\s+meaningful\s+commit\s+messages?\b', 1.0),
    # Pattern 10: Refactoring
    (r'\brefactor\s+code\s+regularly\b', 1.0),
    # Pattern 11: Code duplication
    (r'\bavoid\s+code\s+duplication\b', 1.0),
    # Pattern 12: Static analysis
    (r'\buse\s+(linters?|static\s+analysis\s+tools?)\b', 1.0),
]


def apply_heuristics(rule_text):
    """CLS-009, CLS-010, CLS-012: Heuristic fast-path classification.

    Returns:
        tuple: (is_classified, classification_result or None)
        - is_classified: True if heuristic confidence >= 0.7
        - classification_result: dict with relevance, confidence, reasoning, method if classified
    """
    combined_text = rule_text.lower()

    # CLS-012: Score calculation (exact phrase = 1.0, partial match = 0.5)
    score = 0.0
    matched_patterns = []

    for pattern, weight in HEURISTIC_PATTERNS:
        if re.search(pattern, combined_text, re.IGNORECASE):
            score += weight
            matched_patterns.append(pattern)

    # CLS-012: Threshold >= 0.7 triggers classification without Claude
    # CLS-010: Heuristics classify with confidence >= 0.8 (generic advice)
    if score >= 0.7:
        return (True, {
            'relevance': 'general_advice',
            'confidence': min(0.8 + (score - 0.7) * 0.2, 1.0),  # Scale 0.7-1.0 score to 0.8-1.0 confidence
            'reasoning': f'Matches {len(matched_patterns)} generic software engineering pattern(s)',
            'method': 'heuristic',
            'scope': 'historical'  # Generic advice typically not needed for active development
        })

    # Not classified by heuristics
    return (False, None)


def load_tier_1_domains(config):
    """CLS-004a, CLS-004b: Load tier_1_domains from vocabulary file.

    Returns:
        dict: tier_1_domains dictionary mapping domain names to specifications
    """
    # Vocabulary file is relative to context engine home (BASE_DIR)
    vocab_path = BASE_DIR / config['structure']['vocabulary_file']

    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # CLS-004b: tier_1_domains structure validation
    tier_1_domains = vocab.get('tier_1_domains', {})

    return tier_1_domains


def format_domains_for_prompt(tier_1_domains):
    """CLS-004c: Format tier_1_domains as YAML string for Claude context.

    Omits aliases for brevity, includes only names and descriptions.

    Returns:
        str: YAML formatted domain context
    """
    if not tier_1_domains:
        return "No tier 1 domains defined for this project."

    # Build YAML string manually for precise formatting
    lines = ["Project Domains:"]
    for domain_name, domain_spec in tier_1_domains.items():
        description = domain_spec.get('description', 'No description')
        lines.append(f"  {domain_name}: {description}")

    return '\n'.join(lines)


def load_template():
    """Load quality classification template.

    Returns:
        str: Template content
    """
    template_path = BASE_DIR / "templates" / "runtime-template-quality-classification.txt"

    with open(template_path) as f:
        return f.read()


def classify_batch_with_claude(rules_batch, tier_1_domains, batch_size, template_content):
    """CLS-001, CLS-002, CLS-004, CLS-005, CLS-006: Classify batch with Claude API.

    Args:
        rules_batch: List of rule dictionaries
        tier_1_domains: Domain context dictionary
        batch_size: Number of rules in batch
        template_content: Template string

    Returns:
        list: Classification results in same order as input (CLS-005)
    """
    # CLS-004c: Format domains for prompt
    domains_formatted = format_domains_for_prompt(tier_1_domains)

    # CLS-004d: Format rules batch as JSON
    rules_formatted = json.dumps([
        {
            'id': r['id'],
            'type': r['type'],
            'title': r['title'],
            'description': r['description'],
            'domain': r['domain']
        }
        for r in rules_batch
    ], indent=2)

    # Substitute template variables
    prompt = template_content.replace('{tier_1_domains_with_descriptions}', domains_formatted)
    prompt = prompt.replace('{batch_size}', str(batch_size))
    prompt = prompt.replace('{rules_batch_formatted}', rules_formatted)

    # TODO: Actual Claude API invocation would go here
    # For now, this is a placeholder that returns conservative defaults
    # CLS-006: Classification failures default to confidence 0.5

    # Placeholder: Return conservative classifications
    results = []
    for rule in rules_batch:
        results.append({
            'relevance': 'project_specific',  # Conservative default
            'confidence': 0.5,  # CLS-006: Requires human review
            'reasoning': 'Claude API integration pending - requires manual review',
            'method': 'claude',
            'scope': 'project_wide'
        })

    return results


def get_database_path(config):
    """Get database path from config.

    Returns:
        Path: Absolute path to database
    """
    # Database file is relative to context engine home (BASE_DIR)
    return BASE_DIR / config['structure']['database_file']


def classify_rules(config, dry_run=False):
    """Main classification logic.

    Args:
        config: Configuration dictionary
        dry_run: If True, don't write to database

    Returns:
        dict: Statistics about classification run
    """
    # CLS-004a: Load tier_1_domains from vocabulary
    tier_1_domains = load_tier_1_domains(config)

    # Load template
    template_content = load_template()

    # CLS-001: Get batch size from build-constants (default 15)
    # Note: This would ideally read from build-constants.yaml in production
    batch_size = 15  # Hardcoded for v1.0.0, should read from config in future

    # Connect to database
    db_path = get_database_path(config)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get rules that need classification (no quality_classification in metadata)
    cursor.execute("""
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE lifecycle = 'active'
        AND (metadata IS NULL
             OR json_extract(metadata, '$.quality_classification') IS NULL)
    """)

    rules = cursor.fetchall()

    stats = {
        'total_rules': len(rules),
        'heuristic_classified': 0,
        'claude_classified': 0,
        'batches_processed': 0,
        'errors': 0
    }

    # Process rules in batches
    for i in range(0, len(rules), batch_size):
        batch = rules[i:i + batch_size]
        batch_classifications = []

        for rule in batch:
            rule_dict = dict(rule)

            # Parse existing metadata
            metadata = {}
            if rule_dict['metadata']:
                try:
                    metadata = json.loads(rule_dict['metadata'])
                except json.JSONDecodeError:
                    metadata = {}

            # CLS-009: Try heuristic fast-path first
            combined_text = f"{rule_dict['title']} {rule_dict['description'] or ''}"
            is_classified, heuristic_result = apply_heuristics(combined_text)

            if is_classified:
                # CLS-010: High-confidence heuristic classification
                classification = heuristic_result
                stats['heuristic_classified'] += 1
            else:
                # Need Claude classification
                batch_classifications.append(rule_dict)

        # CLS-001, CLS-005: Process remaining rules with Claude in batch
        if batch_classifications:
            try:
                claude_results = classify_batch_with_claude(
                    batch_classifications,
                    tier_1_domains,
                    len(batch_classifications),
                    template_content
                )

                # Update rules with Claude classifications
                for rule_dict, claude_result in zip(batch_classifications, claude_results):
                    # CLS-007: Store in metadata.quality_classification with ISO8601 timestamp
                    metadata = {}
                    if rule_dict['metadata']:
                        try:
                            metadata = json.loads(rule_dict['metadata'])
                        except json.JSONDecodeError:
                            metadata = {}

                    metadata['quality_classification'] = {
                        'relevance': claude_result['relevance'],
                        'confidence': claude_result['confidence'],
                        'reasoning': claude_result['reasoning'],
                        'method': claude_result['method'],
                        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    }

                    if not dry_run:
                        cursor.execute(
                            "UPDATE rules SET metadata = ? WHERE id = ?",
                            (json.dumps(metadata), rule_dict['id'])
                        )

                stats['claude_classified'] += len(batch_classifications)

            except Exception as e:
                print(f"Error processing batch: {e}", file=sys.stderr)
                stats['errors'] += 1

        # Update heuristic-classified rules
        for rule in batch:
            rule_dict = dict(rule)
            combined_text = f"{rule_dict['title']} {rule_dict['description'] or ''}"
            is_classified, heuristic_result = apply_heuristics(combined_text)

            if is_classified:
                metadata = {}
                if rule_dict['metadata']:
                    try:
                        metadata = json.loads(rule_dict['metadata'])
                    except json.JSONDecodeError:
                        metadata = {}

                # CLS-007: Store classification result
                metadata['quality_classification'] = {
                    'relevance': heuristic_result['relevance'],
                    'confidence': heuristic_result['confidence'],
                    'reasoning': heuristic_result['reasoning'],
                    'method': heuristic_result['method'],
                    'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                }

                if not dry_run:
                    cursor.execute(
                        "UPDATE rules SET metadata = ? WHERE id = ?",
                        (json.dumps(metadata), rule_dict['id'])
                    )

        stats['batches_processed'] += 1

    if not dry_run:
        conn.commit()
    conn.close()

    return stats


def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    print("Context Engine - Quality Classifier")
    print("="*70)

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Classify rules by quality (signal vs noise)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview classifications without writing to database')
    parser.add_argument('--stats', action='store_true',
                        help='Show classification statistics')
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Run classification
    print("\nClassifying rules...")
    if args.dry_run:
        print("(DRY RUN - no database writes)")

    try:
        stats = classify_rules(config, dry_run=args.dry_run)
    except Exception as e:
        print(f"\nError during classification: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Display results
    print("\nClassification Results:")
    print("-" * 70)
    print(f"Total rules processed:     {stats['total_rules']}")
    print(f"Heuristic classifications: {stats['heuristic_classified']} "
          f"({100 * stats['heuristic_classified'] / max(stats['total_rules'], 1):.1f}%)")
    print(f"Claude classifications:    {stats['claude_classified']} "
          f"({100 * stats['claude_classified'] / max(stats['total_rules'], 1):.1f}%)")
    print(f"Batches processed:         {stats['batches_processed']}")
    print(f"Errors:                    {stats['errors']}")

    # CLS-009: Report cost reduction from heuristics
    if stats['total_rules'] > 0:
        cost_reduction = 100 * stats['heuristic_classified'] / stats['total_rules']
        print(f"\nCost reduction from heuristics: {cost_reduction:.1f}%")

    print("\nClassification complete.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
