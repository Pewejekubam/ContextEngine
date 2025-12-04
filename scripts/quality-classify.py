#!/usr/bin/env python3
"""
Quality classifier with heuristic fast-path and Claude batching

Implements constraints: CLS-001 through CLS-012
Generated from: specs/modules/runtime-script-quality-classifier-v1.0.0.yaml
"""

import sys
import json
import sqlite3
import re
import argparse
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timezone

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
# HEURISTIC PATTERNS (CLS-011)
# ============================================================================

# CLS-011: 12 hardcoded generic advice patterns for v1.0.0
HEURISTIC_PATTERNS = [
    r'\buse\s+descriptive\s+(variable|function|class|method)?\s*names?\b',
    r'\bwrite\s+unit\s+tests?\b',
    r'\bfollow\s+best\s+practices?\b',
    r'\bkeep\s+code\s+clean\b',
    r'\bhandle\s+(exceptions?|errors?)\s+gracefully\b',
    r'\bavoid\s+(magic\s+numbers?|hardcoded\s+values?)\b',
    r'\bcomment\s+your\s+code\b|\bdocument\s+functions?\b',
    r'\bfollow\s+(SOLID|DRY)\s+principles?\b',
    r'\buse\s+meaningful\s+commit\s+messages?\b',
    r'\brefactor\s+code\s+regularly\b',
    r'\bavoid\s+code\s+duplication\b',
    r'\buse\s+(linters?|static\s+analysis\s+tools?)\b',
]


def calculate_heuristic_score(text):
    """
    CLS-012: Calculate heuristic match score for rule text.

    Returns:
        float: Score 0.0-1.0 where >= 0.7 triggers classification without Claude
    """
    if not text:
        return 0.0

    # Combine title and description for matching
    search_text = text.lower()

    scores = []
    for pattern in HEURISTIC_PATTERNS:
        match = re.search(pattern, search_text, re.IGNORECASE)
        if match:
            # Check if it's an exact phrase match or partial
            matched_text = match.group(0)
            # Exact phrase gets 1.0, partial match gets 0.5
            if len(matched_text.split()) >= 3:
                scores.append(1.0)
            else:
                scores.append(0.5)

    # Return highest score found
    return max(scores) if scores else 0.0


def classify_with_heuristics(rule):
    """
    CLS-009, CLS-010: Heuristic fast-path classification.

    Returns:
        dict or None: Classification result if confident enough, else None
    """
    text = f"{rule['title']} {rule.get('description', '')}"
    score = calculate_heuristic_score(text)

    # CLS-010: confidence >= 0.8 or <= 0.2 without Claude
    if score >= 0.7:
        # High score = generic advice (CLS-010: confidence 0.8+)
        return {
            'relevance': 'general_advice',
            'confidence': 0.8,
            'reasoning': 'Matches common software engineering platitude patterns',
            'method': 'heuristic',
            'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        }

    # Low scores and medium scores need Claude classification
    return None


# ============================================================================
# VOCABULARY AND TEMPLATE LOADING
# ============================================================================

def load_vocabulary(config):
    """
    CLS-004a: Load tier_1_domains from vocabulary file.

    Returns:
        dict: Vocabulary data including tier_1_domains
    """
    vocab_path = Path(config['structure']['vocabulary_file'])

    if not vocab_path.exists():
        raise FileNotFoundError(f"Vocabulary file not found: {vocab_path}")

    with open(vocab_path) as f:
        return yaml.safe_load(f)


def format_tier1_domains(tier_1_domains):
    """
    CLS-004c: Format tier_1_domains as YAML string with names and descriptions.
    Aliases omitted for brevity.

    Args:
        tier_1_domains: dict mapping domain name to spec dict

    Returns:
        str: YAML formatted domain context
    """
    formatted = {}
    for domain_name, domain_spec in tier_1_domains.items():
        formatted[domain_name] = {
            'description': domain_spec.get('description', '')
        }

    return yaml.dump(formatted, default_flow_style=False, sort_keys=False)


def load_template():
    """Load classification prompt template."""
    template_path = BASE_DIR / "templates" / "runtime-template-quality-classification.txt"

    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    with open(template_path) as f:
        return f.read()


# ============================================================================
# CLAUDE CLASSIFICATION
# ============================================================================

def classify_batch_with_claude(rules_batch, tier_1_domains, batch_size):
    """
    CLS-001, CLS-005: Classify batch of rules with Claude CLI.

    Args:
        rules_batch: list of rule dicts
        tier_1_domains: dict of tier-1 domains
        batch_size: int batch size for template variable

    Returns:
        list: Classification results in same order as input
    """
    # Load template
    template = load_template()

    # CLS-004c: Format tier_1_domains for template
    tier_1_formatted = format_tier1_domains(tier_1_domains)

    # Format rules batch as JSON array
    rules_formatted = json.dumps([
        {
            'rule_id': r['id'],
            'type': r['type'],
            'title': r['title'],
            'description': r.get('description', ''),
            'domain': r.get('domain', '')
        }
        for r in rules_batch
    ], indent=2)

    # Substitute template variables
    prompt = template.replace('{tier_1_domains_with_descriptions}', tier_1_formatted)
    prompt = prompt.replace('{batch_size}', str(batch_size))
    prompt = prompt.replace('{rules_batch_formatted}', rules_formatted)

    # Write prompt to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        prompt_file = f.name
        f.write(prompt)

    try:
        # Invoke Claude CLI
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(prompt_file, 'r'),
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            raise RuntimeError(f"Claude CLI failed: {result.stderr}")

        # Parse JSON from response
        response_text = result.stdout.strip()

        # Extract JSON from markdown code blocks if present
        if '```json' in response_text:
            start = response_text.index('```json') + 7
            end = response_text.index('```', start)
            response_text = response_text[start:end].strip()
        elif '```' in response_text:
            start = response_text.index('```') + 3
            end = response_text.index('```', start)
            response_text = response_text[start:end].strip()

        classifications = json.loads(response_text)

        # CLS-005: Verify array order preservation
        if len(classifications) != len(rules_batch):
            raise ValueError(f"Classification count mismatch: expected {len(rules_batch)}, got {len(classifications)}")

        return classifications

    except (subprocess.TimeoutExpired, json.JSONDecodeError, RuntimeError, ValueError) as e:
        # CLS-006: Failures default to confidence 0.5
        print(f"Warning: Claude classification failed: {e}", file=sys.stderr)
        return [
            {
                'rule_id': r['id'],
                'classification': 'general_advice',
                'confidence': 0.5,
                'scope': 'project_wide',
                'reasoning': 'Classification failed, requires manual review'
            }
            for r in rules_batch
        ]

    finally:
        # Clean up temp file
        Path(prompt_file).unlink(missing_ok=True)


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def get_unclassified_rules(db_path):
    """
    Get all rules without quality classification.

    Returns:
        list: Rule dicts needing classification
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
    SELECT id, type, title, description, domain, metadata
    FROM rules
    WHERE metadata IS NULL
       OR json_extract(metadata, '$.quality_classification') IS NULL
    ORDER BY created_at DESC
    """

    rows = conn.execute(query).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def update_rule_classification(db_path, rule_id, classification):
    """
    CLS-007: Store quality classification in metadata.quality_classification.

    Args:
        db_path: Path to database
        rule_id: Rule ID to update
        classification: dict with relevance, confidence, reasoning, method, classified_at
    """
    conn = sqlite3.connect(db_path)

    # Load existing metadata
    row = conn.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,)).fetchone()

    if row and row[0]:
        metadata = json.loads(row[0])
    else:
        metadata = {}

    # CLS-007: Nest under quality_classification
    metadata['quality_classification'] = classification

    # Update database
    conn.execute(
        "UPDATE rules SET metadata = ? WHERE id = ?",
        (json.dumps(metadata), rule_id)
    )
    conn.commit()
    conn.close()


# ============================================================================
# MAIN CLASSIFICATION LOGIC
# ============================================================================

def classify_rules(db_path, vocabulary, dry_run=False, limit=None):
    """
    Main classification orchestrator.

    Args:
        db_path: Path to database
        vocabulary: Vocabulary dict with tier_1_domains
        dry_run: If True, don't update database
        limit: Optional limit on rules to process

    Returns:
        dict: Statistics about classification run
    """
    # Get unclassified rules
    rules = get_unclassified_rules(db_path)

    if limit:
        rules = rules[:limit]

    if not rules:
        print("No unclassified rules found.")
        return {'total': 0, 'heuristic': 0, 'claude': 0}

    print(f"\nFound {len(rules)} unclassified rules")

    # Load batch size from config
    # Default to 15 if not found (CLS-001)
    try:
        with open(PROJECT_ROOT / 'build' / 'config' / 'build-constants.yaml') as f:
            build_config = yaml.safe_load(f)
            batch_size = build_config.get('tag_optimization', {}).get('classification_batch_size', 15)
    except Exception:
        batch_size = 15

    tier_1_domains = vocabulary.get('tier_1_domains', {})

    heuristic_count = 0
    claude_count = 0

    # Process rules
    rules_needing_claude = []
    heuristic_classifications = {}

    # CLS-009: Try heuristic fast-path first
    print("\n[1/2] Heuristic fast-path classification...")
    for rule in rules:
        classification = classify_with_heuristics(rule)
        if classification:
            heuristic_classifications[rule['id']] = classification
            heuristic_count += 1
            if not dry_run:
                update_rule_classification(db_path, rule['id'], classification)
            print(f"  {rule['id']}: {classification['relevance']} (heuristic, confidence={classification['confidence']})")
        else:
            rules_needing_claude.append(rule)

    print(f"\nHeuristic classified: {heuristic_count}/{len(rules)}")

    # CLS-001: Process remaining rules in batches with Claude
    if rules_needing_claude:
        print(f"\n[2/2] Claude batch classification ({len(rules_needing_claude)} rules)...")

        for i in range(0, len(rules_needing_claude), batch_size):
            batch = rules_needing_claude[i:i+batch_size]
            print(f"\n  Processing batch {i//batch_size + 1} ({len(batch)} rules)...")

            classifications = classify_batch_with_claude(batch, tier_1_domains, len(batch))

            for rule, classification in zip(batch, classifications):
                # Convert to storage format (CLS-007)
                storage_format = {
                    'relevance': classification['classification'],
                    'confidence': classification['confidence'],
                    'reasoning': classification['reasoning'],
                    'method': 'claude',
                    'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                }

                claude_count += 1

                if not dry_run:
                    update_rule_classification(db_path, rule['id'], storage_format)

                print(f"    {rule['id']}: {storage_format['relevance']} (claude, confidence={storage_format['confidence']:.2f})")

    return {
        'total': len(rules),
        'heuristic': heuristic_count,
        'claude': claude_count
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    parser = argparse.ArgumentParser(
        description='Quality classifier with heuristic fast-path and Claude batching'
    )
    parser.add_argument('--dry-run', action='store_true',
                       help='Show classifications without updating database')
    parser.add_argument('--limit', type=int,
                       help='Limit number of rules to classify (for testing)')

    args = parser.parse_args()

    print("Context Engine - Quality Classifier v1.0.0")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1

    # Load vocabulary
    try:
        vocabulary = load_vocabulary(config)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        return 1

    # Get database path
    db_path = Path(config['structure']['database_path'])
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 1

    # Run classification
    try:
        stats = classify_rules(
            db_path=db_path,
            vocabulary=vocabulary,
            dry_run=args.dry_run,
            limit=args.limit
        )

        # Print summary
        print("\n" + "="*70)
        print("Classification Summary")
        print("="*70)
        print(f"Total rules classified: {stats['total']}")
        print(f"  Heuristic (fast-path): {stats['heuristic']}")
        print(f"  Claude (API calls): {stats['claude']}")

        if args.dry_run:
            print("\nDry run - no changes written to database")

        print("\nClassification complete.")

        return 0

    except Exception as e:
        print(f"Error during classification: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
