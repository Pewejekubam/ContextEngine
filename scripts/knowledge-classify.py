#!/usr/bin/env python3
"""
Knowledge type classifier with Claude CLI integration

Implements constraints: KT-001 through KT-046
Generated from: specs/modules/runtime-script-knowledge-classifier-v1.1.0.yaml
"""

import sys
import json
import sqlite3
import subprocess
import tempfile
import argparse
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
# RUNTIME-SCRIPT-KNOWLEDGE-CLASSIFIER MODULE IMPLEMENTATION
# ============================================================================

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

# KT-010a: Default Claude CLI model
DEFAULT_MODEL = 'claude-sonnet-4-5-20250929'


def log_error(error_log_path, rule_id, error_type, details):
    """KT-004e, KT-041: Log error in TSV format"""
    timestamp = datetime.now(timezone.utc).isoformat()
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n")


def normalize_weights(weights, error_log_path, rule_id):
    """KT-005: Normalize weights to sum to 1.0"""
    weight_sum = sum(weights.values())

    # KT-005a: Normalization trigger
    if weight_sum < 0.95 or weight_sum > 1.05:
        # KT-005: Normalization algorithm
        normalized = {k: v / weight_sum for k, v in weights.items()}

        # KT-005c: Normalization logging
        log_error(error_log_path, rule_id, 'NORMALIZED', f'original_sum={weight_sum:.4f} → 1.0')

        return normalized

    return weights


def validate_weights(weights, error_log_path, rule_id):
    """KT-004: Three-stage validation"""
    # Stage 2: All five keys present
    # KT-004b: Missing keys failure
    missing_keys = set(KNOWLEDGE_TYPES) - set(weights.keys())
    if missing_keys:
        log_error(error_log_path, rule_id, 'MISSING_KEYS', f'Missing keys: {missing_keys}')
        return None

    # Ensure all values are floats
    try:
        weights = {k: float(v) for k, v in weights.items()}
    except (ValueError, TypeError) as e:
        log_error(error_log_path, rule_id, 'WEIGHT_SUM_ERROR', f'Invalid weight values: {e}')
        return None

    # Stage 3: Weights sum within tolerance
    weight_sum = sum(weights.values())

    # KT-001a, KT-005a: Weight sum tolerance 0.95-1.05
    if weight_sum < 0.95 or weight_sum > 1.05:
        # KT-004c: Attempt normalization
        normalized = normalize_weights(weights, error_log_path, rule_id)

        # KT-005b: Verify normalization guarantee
        new_sum = sum(normalized.values())
        if 0.9999 <= new_sum <= 1.0001:
            return normalized
        else:
            log_error(error_log_path, rule_id, 'WEIGHT_SUM_ERROR',
                     f'Normalization failed: sum={new_sum:.4f}')
            return None

    return weights


def extract_json_from_response(response_text):
    """KT-010f: Extract JSON from markdown code blocks if present"""
    import re

    # Try to find JSON in markdown code blocks
    code_block_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
    matches = re.findall(code_block_pattern, response_text, re.DOTALL)

    if matches:
        # Return first JSON block found
        return matches[0]

    # Try to find raw JSON object
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    matches = re.findall(json_pattern, response_text, re.DOTALL)

    if matches:
        # Try each match to see if it's valid JSON with our expected keys
        for match in matches:
            try:
                obj = json.loads(match)
                # Check if it has at least some of our expected keys
                if any(k in obj for k in KNOWLEDGE_TYPES):
                    return match
            except json.JSONDecodeError:
                continue

    # Return original if no extraction worked
    return response_text


def classify_rule_with_claude(rule_id, rule_type, title, description, domain,
                              template_path, error_log_path):
    """KT-010: Claude CLI invocation with validation and fallback"""

    # KT-002, KT-002a: Load and populate template
    with open(template_path) as f:
        template = f.read()

    prompt = template.format(
        rule_id=rule_id,
        rule_type=rule_type,
        title=title,
        description=description or '',
        domain=domain or ''
    )

    # KT-010: Create temporary prompt file
    prompt_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            prompt_file = Path(f.name)
            f.write(prompt)

        # KT-010: Claude CLI invocation with timeout
        # KT-010c: 120 seconds timeout
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(prompt_file),
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            log_error(error_log_path, rule_id, 'CLAUDE_CLI_ERROR',
                     f'Exit code {result.returncode}: {result.stderr}')
            return FALLBACK_WEIGHTS, DEFAULT_MODEL

        response = result.stdout.strip()

        # KT-010f: Extract JSON from markdown if needed
        json_text = extract_json_from_response(response)

        # Stage 1: JSON parse success
        # KT-004a: JSON parse failure
        try:
            weights = json.loads(json_text)
        except json.JSONDecodeError as e:
            log_error(error_log_path, rule_id, 'JSON_PARSE_ERROR',
                     f'{e} - Response: {response[:200]}')
            return FALLBACK_WEIGHTS, DEFAULT_MODEL

        # KT-004: Three-stage validation
        validated_weights = validate_weights(weights, error_log_path, rule_id)

        if validated_weights is None:
            # KT-004a-c: Validation failures use fallback
            return FALLBACK_WEIGHTS, DEFAULT_MODEL

        return validated_weights, DEFAULT_MODEL

    except subprocess.TimeoutExpired:
        # KT-010c, KT-042: Timeout error
        log_error(error_log_path, rule_id, 'TIMEOUT',
                 'Claude CLI exceeded 120 second timeout')
        return FALLBACK_WEIGHTS, DEFAULT_MODEL

    except Exception as e:
        # KT-043: All errors trigger fallback
        log_error(error_log_path, rule_id, 'CLAUDE_CLI_ERROR', str(e))
        return FALLBACK_WEIGHTS, DEFAULT_MODEL

    finally:
        # KT-010e: Prompt file cleanup
        if prompt_file and prompt_file.exists():
            prompt_file.unlink()


def get_unclassified_rules(db_path, limit=None):
    """KT-020a: Query for unclassified rules"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # KT-020a: Target selection query
    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL
           OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    # KT-020c: Optional limit flag
    if limit is not None:
        query += f" LIMIT {limit}"

    cursor = conn.cursor()
    cursor.execute(query)
    rules = cursor.fetchall()
    conn.close()

    return rules


def update_rule_metadata(db_path, rule_id, classification_data):
    """KT-030: Update rule metadata with classification"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # KT-003a, KT-007: Build metadata structure
    metadata_patch = json.dumps({
        'knowledge_type': classification_data['weights'],
        'classification_method': 'llm',
        'classification_model': classification_data['model'],
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).isoformat()
    })

    # KT-030, KT-030a, KT-030c: Use json_patch with COALESCE
    cursor.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (metadata_patch, rule_id)
    )

    # KT-030b, KT-020d: Autocommit (commit after each rule)
    conn.commit()
    conn.close()


def format_top_types(weights, top_n=3):
    """KT-020e: Format top N types with weights for progress reporting"""
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    top_types = sorted_types[:top_n]
    return ', '.join([f'{t}={w:.2f}' for t, w in top_types])


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    parser = argparse.ArgumentParser(
        description='Classify rules by knowledge type using Claude CLI'
    )
    parser.add_argument('--limit', type=int, help='Limit number of rules to classify')
    parser.add_argument('--db', help='Path to database file (overrides config)')

    args = parser.parse_args()

    print("Context Engine - Knowledge Classifier")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # KT-002: Template path
    template_path = BASE_DIR / 'templates' / 'runtime-template-knowledge-classification.txt'

    # KT-040: Error log path
    error_log_path = BASE_DIR / 'data' / 'classification_errors.log'

    # Database path
    db_path = args.db if args.db else PROJECT_ROOT / config['paths']['database']

    if not Path(db_path).exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    if not template_path.exists():
        print(f"Error: Template not found at {template_path}", file=sys.stderr)
        sys.exit(1)

    # KT-020: Get unclassified rules
    print(f"\nQuerying unclassified rules from: {db_path}")
    rules = get_unclassified_rules(db_path, limit=args.limit)

    if not rules:
        print("No unclassified rules found.")
        return 0

    print(f"Found {len(rules)} unclassified rules")
    print()

    error_count = 0

    # KT-020: Batch classification mode
    for idx, rule in enumerate(rules, 1):
        rule_id = rule['id']

        print(f"[{idx}/{len(rules)}] Classifying: {rule_id}")

        # KT-010: Classify with Claude CLI
        weights, model = classify_rule_with_claude(
            rule['id'],
            rule['type'],
            rule['title'],
            rule['description'],
            rule['domain'],
            template_path,
            error_log_path
        )

        # Check if fallback was used (indicates error)
        if weights == FALLBACK_WEIGHTS:
            error_count += 1

        # KT-030: Update database
        update_rule_metadata(db_path, rule_id, {
            'weights': weights,
            'model': model
        })

        # KT-020e: Progress reporting
        top_types = format_top_types(weights)
        print(f"  → {top_types}")

    print()
    print("="*70)
    print(f"Classification complete: {len(rules)} rules processed")

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / len(rules)) * 100
        print(f"Errors: {error_count}/{len(rules)} ({percentage:.1f}%) - see {error_log_path}")
        # KT-046: Non-zero exit code if any errors
        return 1
    else:
        print("All classifications successful")
        return 0


if __name__ == '__main__':
    sys.exit(main())
