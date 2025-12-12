#!/usr/bin/env python3
"""
Knowledge type classifier with Claude CLI integration

Implements constraints: KT-001 through KT-046
Generated from: specs/modules/runtime-script-knowledge-classifier-v1.1.0.yaml
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
# RUNTIME-SCRIPT-KNOWLEDGE-CLASSIFIER MODULE IMPLEMENTATION
# ============================================================================

import sqlite3
import subprocess
import tempfile
import re
import argparse
from datetime import datetime, timezone

# KT-001: Five software-native knowledge types
KNOWLEDGE_TYPES = ['reference', 'procedure', 'decision', 'incident', 'pattern']

# KT-004d: Fallback values
FALLBACK_WEIGHTS = {
    'reference': 0.5,
    'procedure': 0.3,
    'decision': 0.2,
    'incident': 0.0,
    'pattern': 0.0
}

# KT-002c: Prompt template version
PROMPT_VERSION = 'v1.0.0'


def log_error(error_log_path: Path, rule_id: str, error_type: str, details: str):
    """KT-004e: Log error in TSV format: {timestamp}\\t{rule_id}\\t{error_type}\\t{details}"""
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(error_log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n")


def normalize_weights(weights: dict, rule_id: str, error_log_path: Path) -> dict:
    """KT-005: Normalization algorithm: normalized_weight[type] = weight[type] / sum(weights)"""
    weight_sum = sum(weights.values())

    # KT-005a: Normalization trigger
    if weight_sum < 0.95 or weight_sum > 1.05:
        # KT-005c: Log normalization
        log_error(error_log_path, rule_id, 'NORMALIZED', f'original_sum={weight_sum} → 1.0')

        # Normalize
        normalized = {k: v / weight_sum for k, v in weights.items()}
        return normalized

    return weights


def validate_classification(response: str, rule_id: str, error_log_path: Path) -> tuple[dict, bool]:
    """KT-004: Three-stage validation: JSON parse, all keys present, weight sum"""

    # KT-010f: Extract JSON from markdown code blocks if needed
    json_str = response.strip()
    if '```json' in json_str or '```' in json_str:
        # Extract JSON from markdown code block
        match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', json_str, re.DOTALL)
        if match:
            json_str = match.group(1)
        else:
            # Try to find any JSON object
            match = re.search(r'\{.*\}', json_str, re.DOTALL)
            if match:
                json_str = match.group(0)

    # Stage 1: JSON parse
    try:
        weights = json.loads(json_str)
    except json.JSONDecodeError as e:
        # KT-004a: JSON parse failure
        log_error(error_log_path, rule_id, 'JSON_PARSE_ERROR', str(e))
        return FALLBACK_WEIGHTS, False

    # Stage 2: All five keys present
    if not all(k in weights for k in KNOWLEDGE_TYPES):
        # KT-004b: Missing keys failure
        missing = [k for k in KNOWLEDGE_TYPES if k not in weights]
        log_error(error_log_path, rule_id, 'MISSING_KEYS', f'missing={missing}')
        return FALLBACK_WEIGHTS, False

    # Stage 3: Weight sum validation
    weight_sum = sum(weights[k] for k in KNOWLEDGE_TYPES)
    if weight_sum < 0.95 or weight_sum > 1.05:
        # KT-004c: Attempt normalization
        normalized = normalize_weights(weights, rule_id, error_log_path)
        return normalized, True

    return weights, True


def classify_with_claude(rule: dict, template_path: Path, error_log_path: Path) -> tuple[dict, bool]:
    """KT-010: Claude CLI invocation with timeout and fallback"""

    # Load and populate template (KT-002, KT-002a)
    with open(template_path) as f:
        template = f.read()

    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'],
        domain=rule.get('domain') or ''
    )

    # Create temporary prompt file
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            temp_file = f.name
            f.write(prompt)

        # KT-010: Claude CLI invocation with KT-010c: 120 second timeout
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(temp_file, 'r'),
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            # KT-010d: Single attempt, use fallback on failure
            log_error(error_log_path, rule['id'], 'CLAUDE_CLI_ERROR',
                     f'exit_code={result.returncode}, stderr={result.stderr}')
            return FALLBACK_WEIGHTS, False

        # Validate and parse response
        weights, success = validate_classification(result.stdout, rule['id'], error_log_path)
        return weights, success

    except subprocess.TimeoutExpired:
        # KT-010c: Timeout handling
        log_error(error_log_path, rule['id'], 'TIMEOUT', 'classification exceeded 120 seconds')
        return FALLBACK_WEIGHTS, False
    except Exception as e:
        log_error(error_log_path, rule['id'], 'CLAUDE_CLI_ERROR', str(e))
        return FALLBACK_WEIGHTS, False
    finally:
        # KT-010e: Cleanup temporary file
        if temp_file and Path(temp_file).exists():
            Path(temp_file).unlink()


def update_rule_metadata(db_path: Path, rule_id: str, weights: dict, model_id: str = 'claude-sonnet-4-5-20250929'):
    """KT-030: Update rules.metadata with classification results"""

    # KT-003a, KT-007: Build metadata structure
    classification_metadata = {
        'knowledge_type': weights,
        'classification_method': 'llm',
        'classification_model': model_id,
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    # KT-030: Use json_patch to merge into existing metadata
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # KT-030c: COALESCE handles NULL metadata
    cursor.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (json.dumps(classification_metadata), rule_id)
    )

    # KT-020d: Commit after each classification
    conn.commit()
    conn.close()


def get_top_types(weights: dict, n=3) -> str:
    """KT-020e: Format top N types with weights for progress reporting"""
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    top_n = sorted_types[:n]
    return ', '.join(f'{k}={v:.2f}' for k, v in top_n)


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    parser = argparse.ArgumentParser(description='Knowledge type classifier')
    parser.add_argument('--limit', type=int, default=None,
                       help='KT-020c: Limit number of rules to classify (for testing)')
    args = parser.parse_args()

    print("Context Engine - Knowledge Type Classifier")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Setup paths
    db_path = PROJECT_ROOT / config['database']['path']
    template_path = BASE_DIR / 'templates' / 'runtime-template-knowledge-classification.txt'
    error_log_path = BASE_DIR / 'data' / 'classification_errors.log'

    # Ensure data directory exists
    error_log_path.parent.mkdir(parents=True, exist_ok=True)

    # KT-020a: Query for unclassified rules
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    if args.limit:
        query += f" LIMIT {args.limit}"

    cursor.execute(query)
    rules = [dict(row) for row in cursor.fetchall()]
    conn.close()

    total = len(rules)
    if total == 0:
        print("\nNo unclassified rules found.")
        return 0

    print(f"\nFound {total} unclassified rule(s)")
    print(f"Template: {template_path}")
    print(f"Error log: {error_log_path}")
    print()

    # Process each rule
    error_count = 0
    for idx, rule in enumerate(rules, 1):
        print(f"[{idx}/{total}] Classifying {rule['id']}...", end=' ')

        # Classify with Claude
        weights, success = classify_with_claude(rule, template_path, error_log_path)

        if not success:
            error_count += 1

        # Update database
        update_rule_metadata(db_path, rule['id'], weights)

        # KT-020e: Progress reporting with top 3 types
        print(get_top_types(weights, 3))

    print()
    print("="*70)
    print("Classification Complete")
    print(f"Total: {total} | Errors: {error_count} | Success: {total - error_count}")

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / total) * 100
        print(f"\nErrors: {error_count}/{total} ({percentage:.1f}%) - see {error_log_path}")

    # KT-046: Exit code based on errors
    return 1 if error_count > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
