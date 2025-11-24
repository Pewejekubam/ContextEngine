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
import anthropic
import os

# CLS-011: 12 hardcoded heuristic patterns (v1.0.0)
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


def load_vocabulary(config):
    """CLS-004a: Load tier_1_domains from vocabulary file.

    CLS-004b: tier_1_domains structure is dictionary mapping domain name to
    domain specification with keys: description (string), aliases (list, optional).
    """
    vocab_path = PROJECT_ROOT / config['structure']['vocabulary_file']
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    return vocab.get('tier_1_domains', {})


def format_domains_for_prompt(tier_1_domains):
    """CLS-004c: Format tier_1_domains as YAML string (omit aliases)."""
    if not tier_1_domains:
        return "No project-specific domains configured."

    lines = []
    for domain_name, domain_spec in tier_1_domains.items():
        description = domain_spec.get('description', 'No description')
        lines.append(f"  {domain_name}: {description}")

    return "\n".join(lines)


def apply_heuristics(rule):
    """CLS-009, CLS-010, CLS-012: Heuristic fast-path filtering."""
    rule_text = f"{rule['title']} {rule.get('description', '')}".lower()

    total_score = 0.0
    matched_patterns = []

    for pattern, weight in HEURISTIC_PATTERNS:
        if re.search(pattern, rule_text, re.IGNORECASE):
            total_score += weight
            matched_patterns.append(pattern)

    # CLS-012: threshold >= 0.7 triggers classification without Claude
    if total_score >= 0.7:
        return {
            'skip_claude': True,
            'classification': 'general_advice',
            'confidence': 0.85,  # CLS-010: High confidence heuristic
            'scope': 'historical',
            'reasoning': f'Generic software engineering advice (matched {len(matched_patterns)} platitude patterns)',
            'method': 'heuristic'
        }

    return {'skip_claude': False}


def load_template():
    """Load quality classification template."""
    template_path = BASE_DIR / "templates" / "runtime-template-quality-classification.txt"
    with open(template_path) as f:
        return f.read()


def classify_batch_with_claude(rules_batch, tier_1_domains, batch_size):
    """CLS-001, CLS-002, CLS-004: Classify batch using Claude API."""
    # Load template
    template = load_template()

    # CLS-004c: Format domains for prompt
    domains_formatted = format_domains_for_prompt(tier_1_domains)

    # Format rules as JSON for prompt
    rules_for_prompt = []
    for rule in rules_batch:
        rules_for_prompt.append({
            'rule_id': rule['id'],
            'type': rule['type'],
            'title': rule['title'],
            'description': rule.get('description', ''),
            'domain': rule.get('domain', '')
        })

    # Substitute template variables
    prompt = template.format(
        tier_1_domains_with_descriptions=domains_formatted,
        batch_size=batch_size,
        rules_batch_formatted=json.dumps(rules_for_prompt, indent=2)
    )

    # Call Claude API
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")

    client = anthropic.Anthropic(api_key=api_key)

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = message.content[0].text.strip()

        # CLS-005: Parse JSON array preserving rule order
        classifications = json.loads(response_text)

        # Map classifications to results
        results = []
        for i, rule in enumerate(rules_batch):
            if i < len(classifications):
                cls = classifications[i]
                results.append({
                    'classification': cls.get('classification', 'noise'),
                    'confidence': cls.get('confidence', 0.5),
                    'scope': cls.get('scope', 'historical'),
                    'reasoning': cls.get('reasoning', 'No reasoning provided'),
                    'method': 'claude'
                })
            else:
                # CLS-006: Default to confidence 0.5 on failure
                results.append({
                    'classification': 'noise',
                    'confidence': 0.5,
                    'scope': 'historical',
                    'reasoning': 'Classification failed: missing result in batch',
                    'method': 'claude'
                })

        return results

    except (json.JSONDecodeError, anthropic.APIError, KeyError) as e:
        # CLS-006: Default to confidence 0.5 on failure
        print(f"Warning: Classification failed for batch: {e}", file=sys.stderr)
        return [{
            'classification': 'noise',
            'confidence': 0.5,
            'scope': 'historical',
            'reasoning': f'Classification failed: {str(e)}',
            'method': 'claude'
        } for _ in rules_batch]


def update_rule_metadata(db_path, rule_id, classification_result):
    """CLS-007: Store quality classification in metadata.quality_classification.

    CLS-003: Classification confidence < 0.7 prevents auto-approval in subsequent
    optimization (tag optimizer reads metadata.quality_classification.confidence).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Fetch current metadata
        cursor.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,))
        row = cursor.fetchone()

        if not row:
            print(f"Warning: Rule {rule_id} not found", file=sys.stderr)
            return

        metadata_json = row[0] or "{}"
        metadata = json.loads(metadata_json)

        # CLS-007: Store under quality_classification namespace
        metadata['quality_classification'] = {
            'relevance': classification_result['classification'],
            'confidence': classification_result['confidence'],
            'reasoning': classification_result['reasoning'],
            'method': classification_result['method'],
            'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        }

        # Update database
        cursor.execute(
            "UPDATE rules SET metadata = ? WHERE id = ?",
            (json.dumps(metadata), rule_id)
        )
        conn.commit()

    finally:
        conn.close()


def get_unclassified_rules(db_path):
    """Fetch rules that need quality classification."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get rules without quality_classification in metadata
    cursor.execute("""
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE lifecycle = 'active'
        ORDER BY id
    """)

    rules = []
    for row in cursor.fetchall():
        rule_id, rule_type, title, description, domain, metadata_json = row

        metadata = json.loads(metadata_json or "{}")

        # Skip if already classified
        if 'quality_classification' in metadata:
            continue

        rules.append({
            'id': rule_id,
            'type': rule_type,
            'title': title,
            'description': description,
            'domain': domain
        })

    conn.close()
    return rules


def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    print("Context Engine - Runtime-script-quality-classifier Module")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # CLS-001: Get batch size from config
    # Note: In actual deployment this would come from build-constants.yaml via config
    # For now, use default from spec
    batch_size = 15

    # CLS-004a: Load vocabulary
    try:
        tier_1_domains = load_vocabulary(config)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        tier_1_domains = {}

    # Get database path
    db_path = PROJECT_ROOT / config['structure']['database_file']

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    # Get unclassified rules
    print("\nFetching unclassified rules...")
    rules = get_unclassified_rules(db_path)

    if not rules:
        print("No rules need classification.")
        return 0

    print(f"Found {len(rules)} rules to classify")

    # Process rules
    total_classified = 0
    heuristic_classified = 0
    claude_classified = 0

    # CLS-001: Process in batches
    for i in range(0, len(rules), batch_size):
        batch = rules[i:i+batch_size]

        print(f"\nProcessing batch {i//batch_size + 1} ({len(batch)} rules)...")

        # CLS-009: Apply heuristics first
        batch_results = []
        rules_for_claude = []
        heuristic_results = []

        for rule in batch:
            heuristic_result = apply_heuristics(rule)

            if heuristic_result['skip_claude']:
                batch_results.append((rule, heuristic_result))
                heuristic_classified += 1
            else:
                rules_for_claude.append(rule)

        # CLS-001, CLS-002: Classify remaining rules with Claude
        if rules_for_claude:
            print(f"  Heuristic classified: {len(batch) - len(rules_for_claude)}")
            print(f"  Sending {len(rules_for_claude)} rules to Claude...")

            claude_results = classify_batch_with_claude(
                rules_for_claude,
                tier_1_domains,
                len(rules_for_claude)
            )

            for rule, result in zip(rules_for_claude, claude_results):
                batch_results.append((rule, result))
                claude_classified += 1
        else:
            print(f"  All {len(batch)} rules classified by heuristics")

        # CLS-007: Store results
        for rule, result in batch_results:
            update_rule_metadata(db_path, rule['id'], result)
            total_classified += 1

    print("\n" + "="*70)
    print(f"Classification complete:")
    print(f"  Total classified: {total_classified}")
    print(f"  Heuristic fast-path: {heuristic_classified} ({heuristic_classified/total_classified*100:.1f}%)")
    print(f"  Claude API calls: {claude_classified} ({claude_classified/total_classified*100:.1f}%)")
    print(f"  API batches: {(claude_classified + batch_size - 1) // batch_size}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
