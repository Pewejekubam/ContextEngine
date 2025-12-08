#!/usr/bin/env python3
"""
Quality classifier with heuristic fast-path and Claude batching

Implements constraints: CLS-001 through CLS-012
Generated from: specs/modules/runtime-script-quality-classifier-v1.0.0.yaml
"""

import sys
import json
import sqlite3
import argparse
import re
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, UTC

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

# CLS-011: 12 hardcoded generic advice patterns
HEURISTIC_PATTERNS = [
    # 1. Descriptive naming
    r'\buse\s+descriptive\s+(variable|function|class|method)?\s*names?\b',
    # 2. Unit testing
    r'\bwrite\s+unit\s+tests?\b',
    # 3. Best practices
    r'\bfollow\s+best\s+practices?\b',
    # 4. Code cleanliness
    r'\bkeep\s+code\s+clean\b',
    # 5. Error handling
    r'\bhandle\s+(exceptions?|errors?)\s+gracefully\b',
    # 6. Magic numbers
    r'\bavoid\s+(magic\s+numbers?|hardcoded\s+values?)\b',
    # 7. Documentation
    r'\bcomment\s+your\s+code\b|\bdocument\s+functions?\b',
    # 8. Design principles
    r'\bfollow\s+(SOLID|DRY)\s+principles?\b',
    # 9. Commit messages
    r'\buse\s+meaningful\s+commit\s+messages?\b',
    # 10. Refactoring
    r'\brefactor\s+code\s+regularly\b',
    # 11. Code duplication
    r'\bavoid\s+code\s+duplication\b',
    # 12. Static analysis
    r'\buse\s+(linters?|static\s+analysis\s+tools?)\b',
]


def apply_heuristics(rule):
    """
    CLS-009, CLS-010, CLS-011, CLS-012: Fast-path heuristic classification

    Returns:
        dict with keys: matched (bool), confidence (float), reasoning (str)
        or None if no heuristic match
    """
    text = f"{rule['title']} {rule['description']}".lower()

    # CLS-012: Track exact and partial matches
    exact_matches = 0
    partial_matches = 0

    for pattern in HEURISTIC_PATTERNS:
        regex = re.compile(pattern, re.IGNORECASE)
        match = regex.search(text)
        if match:
            # Check if match is exact phrase or partial
            matched_text = match.group(0)
            # Simple heuristic: if match is < 60% of text, it's partial
            if len(matched_text) < len(text) * 0.6:
                partial_matches += 1
            else:
                exact_matches += 1

    # CLS-012: Score rules - exact phrase = 1.0, partial match = 0.5
    score = exact_matches * 1.0 + partial_matches * 0.5

    # CLS-012: threshold >= 0.7 triggers classification without Claude
    if score >= 0.7:
        # CLS-010: Classify as general_advice with high confidence
        return {
            'matched': True,
            'relevance': 'general_advice',
            'confidence': min(0.95, 0.7 + (score - 0.7) * 0.1),  # Scale 0.7-0.95
            'scope': 'historical',
            'reasoning': f'Generic software engineering advice (heuristic score: {score:.1f})'
        }

    return None


# ============================================================================
# VOCABULARY LOADING (CLS-004a, CLS-004b)
# ============================================================================

def load_tier_1_domains(config):
    """
    CLS-004a, CLS-004b: Load tier_1_domains from vocabulary file

    Returns:
        dict mapping domain name to domain specification
    """
    vocab_path = BASE_DIR / config['structure']['vocabulary_file']

    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab.get('tier_1_domains', {})
    except Exception as e:
        print(f"Warning: Failed to load vocabulary: {e}", file=sys.stderr)
        return {}


def format_tier_1_domains(tier_1_domains):
    """
    CLS-004c: Format tier_1_domains as YAML string with descriptions

    Aliases omitted for brevity
    """
    if not tier_1_domains:
        return "No domains configured"

    lines = []
    for domain_name, domain_spec in tier_1_domains.items():
        description = domain_spec.get('description', 'No description')
        lines.append(f"  {domain_name}: {description}")

    return "\n".join(lines)


# ============================================================================
# CLAUDE CLASSIFICATION (CLS-001, CLS-002, CLS-004, CLS-005, CLS-006)
# ============================================================================

def classify_batch_with_claude(rules_batch, tier_1_domains_formatted, batch_size, template_path):
    """
    CLS-001, CLS-002, CLS-004, CLS-005, CLS-006: Batch classification via Claude

    Args:
        rules_batch: list of rule dicts
        tier_1_domains_formatted: YAML formatted domain context
        batch_size: number of rules in batch
        template_path: path to classification template

    Returns:
        list of classification results (one per rule)
    """
    # Format rules for prompt
    rules_formatted = []
    for rule in rules_batch:
        rules_formatted.append({
            'rule_id': rule['id'],
            'type': rule['type'],
            'title': rule['title'],
            'description': rule['description'],
            'domain': rule['domain']
        })

    rules_batch_json = json.dumps(rules_formatted, indent=2)

    # Load template
    try:
        with open(template_path) as f:
            template = f.read()
    except Exception as e:
        print(f"Error loading template: {e}", file=sys.stderr)
        return [default_classification(rule) for rule in rules_batch]

    # Substitute variables
    prompt = template.format(
        tier_1_domains_with_descriptions=tier_1_domains_formatted,
        batch_size=batch_size,
        rules_batch_formatted=rules_batch_json
    )

    # Call Claude CLI
    try:
        # Write prompt to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(prompt)
            prompt_file = f.name

        # Invoke Claude CLI
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(prompt_file),
            capture_output=True,
            text=True,
            timeout=120
        )

        # Clean up temp file
        Path(prompt_file).unlink()

        if result.returncode != 0:
            print(f"Warning: Claude CLI failed: {result.stderr}", file=sys.stderr)
            return [default_classification(rule) for rule in rules_batch]

        # Parse response
        response = result.stdout.strip()

        # Extract JSON from markdown if needed
        if '```json' in response:
            start = response.find('```json') + 7
            end = response.find('```', start)
            response = response[start:end].strip()
        elif '```' in response:
            start = response.find('```') + 3
            end = response.find('```', start)
            response = response[start:end].strip()

        # Parse JSON
        try:
            classifications = json.loads(response)

            # CLS-005: Validate array order preservation
            if len(classifications) != len(rules_batch):
                print(f"Warning: Classification count mismatch ({len(classifications)} != {len(rules_batch)})", file=sys.stderr)
                return [default_classification(rule) for rule in rules_batch]

            # Map classifications to rules
            results = []
            for i, classification in enumerate(classifications):
                rule = rules_batch[i]
                results.append({
                    'rule_id': rule['id'],
                    'relevance': classification.get('classification', 'noise'),
                    'confidence': float(classification.get('confidence', 0.5)),
                    'scope': classification.get('scope', 'historical'),
                    'reasoning': classification.get('reasoning', 'No reasoning provided')
                })

            return results

        except json.JSONDecodeError as e:
            # CLS-006: JSON parse failure defaults to confidence 0.5
            print(f"Warning: Failed to parse Claude response: {e}", file=sys.stderr)
            return [default_classification(rule) for rule in rules_batch]

    except subprocess.TimeoutExpired:
        # CLS-006: Timeout defaults to confidence 0.5
        print("Warning: Claude CLI timeout", file=sys.stderr)
        Path(prompt_file).unlink(missing_ok=True)
        return [default_classification(rule) for rule in rules_batch]

    except Exception as e:
        # CLS-006: Any failure defaults to confidence 0.5
        print(f"Warning: Classification error: {e}", file=sys.stderr)
        Path(prompt_file).unlink(missing_ok=True)
        return [default_classification(rule) for rule in rules_batch]


def default_classification(rule):
    """CLS-006: Default classification for failures (confidence 0.5)"""
    return {
        'rule_id': rule['id'],
        'relevance': 'noise',
        'confidence': 0.5,
        'scope': 'historical',
        'reasoning': 'Classification failed, requires manual review'
    }


# ============================================================================
# DATABASE OPERATIONS (CLS-007)
# ============================================================================

def get_unclassified_rules(conn):
    """Query rules without quality classification"""
    cursor = conn.execute("""
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL
           OR json_extract(metadata, '$.quality_classification') IS NULL
        ORDER BY created_at DESC
    """)

    rules = []
    for row in cursor.fetchall():
        rules.append({
            'id': row['id'],
            'type': row['type'],
            'title': row['title'],
            'description': row['description'],
            'domain': row['domain'],
            'metadata': json.loads(row['metadata']) if row['metadata'] else {}
        })

    return rules


def update_rule_classification(conn, rule_id, classification, method):
    """
    CLS-007: Store quality classification in metadata.quality_classification

    Args:
        rule_id: rule ID
        classification: dict with relevance, confidence, scope, reasoning
        method: 'heuristic' or 'claude'
    """
    # Get existing metadata
    cursor = conn.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,))
    row = cursor.fetchone()
    metadata = json.loads(row['metadata']) if row and row['metadata'] else {}

    # CLS-007: Add quality_classification nested structure
    metadata['quality_classification'] = {
        'relevance': classification['relevance'],
        'confidence': classification['confidence'],
        'reasoning': classification['reasoning'],
        'method': method,
        'classified_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z')
    }

    # Update database
    conn.execute(
        "UPDATE rules SET metadata = ? WHERE id = ?",
        (json.dumps(metadata), rule_id)
    )
    conn.commit()


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    parser = argparse.ArgumentParser(description='Quality classification with heuristic fast-path')
    parser.add_argument('--limit', type=int, help='Limit number of rules to classify')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    args = parser.parse_args()

    print("Context Engine - Quality Classifier")
    print("=" * 70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Connect to database
    db_path = BASE_DIR / config['structure']['database_path']
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Load tier_1_domains (CLS-004a, CLS-004b)
    tier_1_domains = load_tier_1_domains(config)
    tier_1_domains_formatted = format_tier_1_domains(tier_1_domains)

    # Load batch size from build constants (CLS-001)
    try:
        constants_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'
        with open(constants_path) as f:
            constants = yaml.safe_load(f)
        batch_size = constants.get('tag_optimization', {}).get('classification_batch_size', 15)
    except Exception:
        batch_size = 15  # Default

    # Get template path
    template_path = BASE_DIR / 'templates' / 'runtime-template-quality-classification.txt'
    if not template_path.exists():
        print(f"Error: Template not found: {template_path}", file=sys.stderr)
        sys.exit(1)

    # Get unclassified rules
    rules = get_unclassified_rules(conn)

    if args.limit:
        rules = rules[:args.limit]

    if not rules:
        print("No unclassified rules found.")
        return 0

    print(f"Found {len(rules)} unclassified rules")

    # Statistics
    stats = {
        'total': len(rules),
        'heuristic_matches': 0,
        'claude_classifications': 0,
        'project_specific': 0,
        'general_advice': 0,
        'noise': 0
    }

    # Process rules
    heuristic_classified = []
    needs_claude = []

    # CLS-009: Heuristic fast-path filtering
    print("\n[1/3] Heuristic Fast-Path")
    print("-" * 70)
    for rule in rules:
        heuristic_result = apply_heuristics(rule)
        if heuristic_result and heuristic_result.get('matched'):
            heuristic_classified.append((rule, heuristic_result))
            stats['heuristic_matches'] += 1
            if args.verbose:
                print(f"  {rule['id']}: {heuristic_result['relevance']} (conf={heuristic_result['confidence']:.2f})")
        else:
            needs_claude.append(rule)

    print(f"Heuristic matches: {stats['heuristic_matches']}/{stats['total']} ({stats['heuristic_matches']/stats['total']*100:.1f}%)")

    # Store heuristic classifications
    for rule, classification in heuristic_classified:
        update_rule_classification(conn, rule['id'], classification, method='heuristic')
        stats[classification['relevance']] += 1

    # CLS-001: Batch classification with Claude
    if needs_claude:
        print(f"\n[2/3] Claude Batch Classification ({len(needs_claude)} rules)")
        print("-" * 70)

        for i in range(0, len(needs_claude), batch_size):
            batch = needs_claude[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(needs_claude) + batch_size - 1) // batch_size

            print(f"  Batch {batch_num}/{total_batches} ({len(batch)} rules)...")

            classifications = classify_batch_with_claude(
                batch,
                tier_1_domains_formatted,
                len(batch),
                template_path
            )

            # Store classifications
            for classification in classifications:
                update_rule_classification(
                    conn,
                    classification['rule_id'],
                    classification,
                    method='claude'
                )
                stats['claude_classifications'] += 1
                stats[classification['relevance']] += 1

                if args.verbose:
                    print(f"    {classification['rule_id']}: {classification['relevance']} (conf={classification['confidence']:.2f})")

    # Final summary
    print("\n[3/3] Classification Summary")
    print("-" * 70)
    print(f"Total rules classified: {stats['total']}")
    print(f"  - Heuristic matches: {stats['heuristic_matches']} ({stats['heuristic_matches']/stats['total']*100:.1f}%)")
    print(f"  - Claude classifications: {stats['claude_classifications']} ({stats['claude_classifications']/stats['total']*100:.1f}%)")
    print()
    print("Classification breakdown:")
    print(f"  - project_specific: {stats['project_specific']}")
    print(f"  - general_advice: {stats['general_advice']}")
    print(f"  - noise: {stats['noise']}")

    # CLS-003: Warn about low-confidence classifications
    cursor = conn.execute("""
        SELECT COUNT(*) as count
        FROM rules
        WHERE json_extract(metadata, '$.quality_classification.confidence') < 0.7
    """)
    low_confidence_count = cursor.fetchone()['count']
    if low_confidence_count > 0:
        print(f"\nWarning: {low_confidence_count} rules classified with confidence < 0.7 (requires review)")

    conn.close()
    print("\nModule execution complete.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
