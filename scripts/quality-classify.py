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
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

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

# CLS-011: 12 hardcoded heuristic patterns for generic advice detection
HEURISTIC_PATTERNS = [
    # Pattern, confidence weight for match
    (r'\buse\s+descriptive\s+(variable|function|class|method)?\s*names?\b', 1.0),
    (r'\bwrite\s+unit\s+tests?\b', 1.0),
    (r'\bfollow\s+best\s+practices?\b', 1.0),
    (r'\bkeep\s+code\s+clean\b', 1.0),
    (r'\bhandle\s+(exceptions?|errors?)\s+gracefully\b', 1.0),
    (r'\bavoid\s+(magic\s+numbers?|hardcoded\s+values?)\b', 1.0),
    (r'\bcomment\s+your\s+code\b', 1.0),
    (r'\bdocument\s+functions?\b', 1.0),
    (r'\bfollow\s+(SOLID|DRY)\s+principles?\b', 1.0),
    (r'\buse\s+meaningful\s+commit\s+messages?\b', 1.0),
    (r'\brefactor\s+code\s+regularly\b', 1.0),
    (r'\bavoid\s+code\s+duplication\b', 1.0),
    (r'\buse\s+(linters?|static\s+analysis\s+tools?)\b', 1.0),
]


def load_vocabulary(config: dict) -> dict:
    """
    CLS-004a: Load tier_1_domains from vocabulary file.

    Returns vocabulary dict with tier_1_domains structure.
    """
    vocab_path = BASE_DIR / "config" / "tag-vocabulary.yaml"

    if not vocab_path.exists():
        print(f"Error: Vocabulary file not found: {vocab_path}", file=sys.stderr)
        sys.exit(1)

    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    if 'tier_1_domains' not in vocab:
        print(f"Error: Vocabulary file missing tier_1_domains", file=sys.stderr)
        sys.exit(1)

    return vocab


def format_tier1_domains_for_prompt(vocab: dict) -> str:
    """
    CLS-004c: Format tier_1_domains as YAML string with names and descriptions.
    Aliases omitted for brevity.
    """
    domains = vocab.get('tier_1_domains', {})

    # Build YAML-like output
    lines = []
    for domain_name, domain_spec in domains.items():
        description = domain_spec.get('description', 'No description')
        lines.append(f"{domain_name}: {description}")

    return "\n".join(lines)


def apply_heuristics(rule: dict) -> Tuple[Optional[str], float, Optional[str]]:
    """
    CLS-009, CLS-010, CLS-011, CLS-012: Heuristic fast-path filtering.

    Returns: (relevance, confidence, reasoning) or (None, 0.0, None) if no match

    - CLS-011: Match 12 hardcoded patterns
    - CLS-012: Score 1.0 for exact match, 0.5 for partial
    - CLS-010: Confidence >= 0.8 triggers classification without Claude
    """
    text = f"{rule.get('title', '')} {rule.get('description', '')}".lower()

    max_score = 0.0
    matched_pattern = None

    for pattern, weight in HEURISTIC_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Exact phrase match = full weight, otherwise partial
            match_text = match.group(0)
            # If match covers significant portion of title/description, it's exact
            score = weight if len(match_text) > 10 else weight * 0.5

            if score > max_score:
                max_score = score
                matched_pattern = pattern

    # CLS-012: Threshold >= 0.7 triggers classification without Claude
    if max_score >= 0.7:
        # CLS-010: High confidence (>= 0.8) generic advice classification
        confidence = min(max_score, 1.0)
        relevance = 'general_advice'
        reasoning = f"Matched generic advice pattern: {matched_pattern}"
        return relevance, confidence, reasoning

    # No strong heuristic match
    return None, 0.0, None


def get_rules_needing_classification(db_path: Path, limit: Optional[int] = None) -> List[dict]:
    """
    Query rules that need quality classification.

    Returns rules where metadata.quality_classification is NULL or missing.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT id, type, title, description, domain, tags, metadata
        FROM rules
        WHERE json_extract(metadata, '$.quality_classification') IS NULL
        ORDER BY created_at DESC
    """

    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def batch_classify_with_claude(rules: List[dict], vocab: dict, config: dict,
                               batch_size: int = 15) -> List[dict]:
    """
    CLS-001, CLS-002, CLS-004, CLS-005, CLS-006: Batch classification with Claude.

    Returns list of classification results matching input order.
    """
    # Load template
    template_path = BASE_DIR / "templates" / "runtime-template-quality-classification.txt"

    if not template_path.exists():
        print(f"Error: Template not found: {template_path}", file=sys.stderr)
        sys.exit(1)

    with open(template_path) as f:
        template_content = f.read()

    # CLS-004c: Format tier_1_domains for prompt
    tier1_formatted = format_tier1_domains_for_prompt(vocab)

    # Process in batches
    all_results = []

    for i in range(0, len(rules), batch_size):
        batch = rules[i:i+batch_size]

        # Format batch for prompt
        rules_formatted = []
        for rule in batch:
            rules_formatted.append({
                'id': rule['id'],
                'type': rule['type'],
                'title': rule['title'],
                'description': rule.get('description', ''),
                'domain': rule.get('domain', 'unknown')
            })

        # Substitute template variables
        prompt = template_content.format(
            batch_size=len(batch),
            tier_1_domains_with_descriptions=tier1_formatted,
            rules_batch_formatted=json.dumps(rules_formatted, indent=2)
        )

        # Call Claude via CLI
        try:
            result = subprocess.run(
                ['claude', '-m', 'claude-sonnet-4-5-20250929'],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode != 0:
                # CLS-006: Classification failure defaults to confidence 0.5
                print(f"Warning: Claude CLI failed for batch {i//batch_size + 1}", file=sys.stderr)
                for rule in batch:
                    all_results.append({
                        'rule_id': rule['id'],
                        'classification': 'general_advice',
                        'confidence': 0.5,
                        'scope': 'project_wide',
                        'reasoning': 'Classification failed - requires manual review'
                    })
                continue

            # Parse JSON response
            response_text = result.stdout.strip()

            # Try to extract JSON from markdown code blocks
            json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', response_text, re.DOTALL)
            if json_match:
                response_text = json_match.group(1)

            # CLS-005: Parse JSON array
            try:
                classifications = json.loads(response_text)

                # Validate we got the right number of results
                if len(classifications) != len(batch):
                    print(f"Warning: Expected {len(batch)} classifications, got {len(classifications)}",
                          file=sys.stderr)
                    # CLS-006: Default to 0.5 confidence
                    for rule in batch:
                        all_results.append({
                            'rule_id': rule['id'],
                            'classification': 'general_advice',
                            'confidence': 0.5,
                            'scope': 'project_wide',
                            'reasoning': 'Batch size mismatch - requires review'
                        })
                    continue

                all_results.extend(classifications)

            except json.JSONDecodeError as e:
                # CLS-006: Malformed JSON defaults to confidence 0.5
                print(f"Warning: JSON parse failed for batch {i//batch_size + 1}: {e}",
                      file=sys.stderr)
                for rule in batch:
                    all_results.append({
                        'rule_id': rule['id'],
                        'classification': 'general_advice',
                        'confidence': 0.5,
                        'scope': 'project_wide',
                        'reasoning': 'JSON parse error - requires review'
                    })

        except subprocess.TimeoutExpired:
            # CLS-006: Timeout defaults to confidence 0.5
            print(f"Warning: Claude timeout for batch {i//batch_size + 1}", file=sys.stderr)
            for rule in batch:
                all_results.append({
                    'rule_id': rule['id'],
                    'classification': 'general_advice',
                    'confidence': 0.5,
                    'scope': 'project_wide',
                    'reasoning': 'Classification timeout - requires review'
                })

    return all_results


def update_rule_classification(db_path: Path, rule_id: str, classification: dict,
                               method: str = 'claude'):
    """
    CLS-007: Store quality classification in metadata.quality_classification.

    Structure: {
        quality_classification: {
            relevance: 'project_specific | general_advice | noise',
            confidence: 0.0-1.0,
            reasoning: 'explanation',
            method: 'heuristic | claude',
            classified_at: 'ISO8601 UTC with Z suffix'
        }
    }
    """
    conn = sqlite3.connect(db_path)

    # Get current metadata
    row = conn.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,)).fetchone()

    if row is None:
        conn.close()
        print(f"Warning: Rule {rule_id} not found", file=sys.stderr)
        return

    # Parse existing metadata
    try:
        metadata = json.loads(row[0]) if row[0] else {}
    except json.JSONDecodeError:
        metadata = {}

    # CLS-007: Create quality_classification structure
    metadata['quality_classification'] = {
        'relevance': classification.get('classification', 'general_advice'),
        'confidence': classification.get('confidence', 0.5),
        'reasoning': classification.get('reasoning', 'No reasoning provided'),
        'method': method,
        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    # Update database
    conn.execute(
        "UPDATE rules SET metadata = ? WHERE id = ?",
        (json.dumps(metadata), rule_id)
    )
    conn.commit()
    conn.close()


def main():
    """Hybrid heuristic + Claude quality classification for noise filtering before tag optimization"""
    parser = argparse.ArgumentParser(
        description='Quality classifier with heuristic fast-path and Claude batching'
    )
    parser.add_argument('--limit', type=int, help='Limit number of rules to classify')
    parser.add_argument('--batch-size', type=int, default=15,
                       help='Batch size for Claude API calls (default: 15)')
    parser.add_argument('--heuristic-only', action='store_true',
                       help='Only apply heuristics, skip Claude classification')
    args = parser.parse_args()

    print("Context Engine - Quality Classification")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Load vocabulary
    try:
        vocab = load_vocabulary(config)
    except Exception as e:
        print(f"Error loading vocabulary: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path
    db_path = BASE_DIR / config['structure']['database_path']

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Get rules needing classification
    print(f"\nQuerying rules needing classification...")
    rules = get_rules_needing_classification(db_path, args.limit)

    if not rules:
        print("No rules need classification.")
        return 0

    print(f"Found {len(rules)} rules needing classification.")

    # CLS-009: Apply heuristics first for fast-path filtering
    heuristic_classified = []
    claude_needed = []

    print("\nApplying heuristic filters...")
    for rule in rules:
        relevance, confidence, reasoning = apply_heuristics(rule)

        if relevance:  # CLS-010: Confidence >= 0.8 from heuristics
            heuristic_classified.append({
                'rule': rule,
                'classification': {
                    'rule_id': rule['id'],
                    'classification': relevance,
                    'confidence': confidence,
                    'scope': 'project_wide',
                    'reasoning': reasoning
                }
            })
        else:
            claude_needed.append(rule)

    print(f"  Heuristic matches: {len(heuristic_classified)}")
    print(f"  Require Claude: {len(claude_needed)}")

    # Store heuristic classifications
    for item in heuristic_classified:
        update_rule_classification(
            db_path,
            item['rule']['id'],
            item['classification'],
            method='heuristic'
        )

    # CLS-001: Batch classify remaining rules with Claude
    if claude_needed and not args.heuristic_only:
        print(f"\nClassifying {len(claude_needed)} rules with Claude (batch size: {args.batch_size})...")

        classifications = batch_classify_with_claude(
            claude_needed,
            vocab,
            config,
            batch_size=args.batch_size
        )

        # Store Claude classifications
        for classification in classifications:
            update_rule_classification(
                db_path,
                classification['rule_id'],
                classification,
                method='claude'
            )

        print(f"  Completed {len(classifications)} Claude classifications")
    elif claude_needed and args.heuristic_only:
        print(f"\nSkipping Claude classification (--heuristic-only mode)")

    # Summary
    print("\n" + "="*70)
    print("Classification Summary:")
    print(f"  Total processed: {len(rules)}")
    print(f"  Heuristic classifications: {len(heuristic_classified)}")
    print(f"  Claude classifications: {len(claude_needed) if not args.heuristic_only else 0}")
    print(f"  Cost reduction: {(len(heuristic_classified) / len(rules) * 100):.1f}%")

    # CLS-003: Note about confidence thresholds
    print("\nNote: Rules with confidence < 0.7 will require manual review")
    print("      before auto-approval in tag optimization.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
