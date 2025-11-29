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
import sqlite3
import subprocess
import tempfile
import os
import re
from datetime import datetime, timezone
import argparse

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

# KT-010c: Timeout per classification
CLASSIFICATION_TIMEOUT = 120


def load_config():
    """Load deployment configuration and vocabulary."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


# ============================================================================
# RUNTIME-SCRIPT-KNOWLEDGE-CLASSIFIER MODULE IMPLEMENTATION
# ============================================================================


def log_error(error_log_path, rule_id, error_type, details):
    """KT-041, KT-004e: Log error in TSV format"""
    timestamp = datetime.now(timezone.utc).isoformat()
    # Ensure data directory exists
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n")


def normalize_weights(weights, rule_id, error_log_path):
    """KT-005: Normalize weights to sum to 1.0"""
    total = sum(weights.values())

    # KT-005a: Normalization trigger
    if total < 0.95 or total > 1.05:
        # KT-005: Normalization algorithm
        normalized = {k: v / total for k, v in weights.items()}

        # KT-005c: Normalization logging
        log_error(error_log_path, rule_id, 'NORMALIZED', f'original_sum={total} → 1.0')

        return normalized

    return weights


def validate_weights(weights, rule_id, error_log_path):
    """KT-004: Three-stage validation"""

    # Stage 2: All five keys present (KT-004b)
    missing_keys = set(KNOWLEDGE_TYPES) - set(weights.keys())
    if missing_keys:
        log_error(error_log_path, rule_id, 'MISSING_KEYS', f'Missing keys: {missing_keys}')
        return FALLBACK_WEIGHTS.copy()

    # Ensure all weights are numeric
    try:
        weights = {k: float(v) for k, v in weights.items()}
    except (ValueError, TypeError) as e:
        log_error(error_log_path, rule_id, 'WEIGHT_SUM_ERROR', f'Non-numeric weights: {e}')
        return FALLBACK_WEIGHTS.copy()

    # Stage 3: Weights sum within tolerance (KT-004c)
    total = sum(weights.values())
    if total < 0.95 or total > 1.05:
        # Attempt normalization
        normalized = normalize_weights(weights, rule_id, error_log_path)
        # Verify normalization worked (KT-005b)
        normalized_sum = sum(normalized.values())
        if 0.9999 <= normalized_sum <= 1.0001:
            return normalized
        else:
            log_error(error_log_path, rule_id, 'WEIGHT_SUM_ERROR',
                     f'Normalization failed: sum={normalized_sum}')
            return FALLBACK_WEIGHTS.copy()

    return weights


def extract_json_from_markdown(text):
    """KT-010f: Extract JSON from markdown code blocks"""
    # Try to find JSON in markdown code blocks
    code_block_pattern = r'```(?:json)?\s*\n(.*?)\n```'
    matches = re.findall(code_block_pattern, text, re.DOTALL)

    if matches:
        # Try each code block until we find valid JSON
        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue

    # If no code blocks, try to parse the entire text as JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def classify_with_claude(rule, template_path, error_log_path):
    """KT-010: Classify rule using Claude CLI"""

    # KT-002: Load prompt template
    with open(template_path) as f:
        template = f.read()

    # KT-002a: Template variables
    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'] or '',
        description=rule['description'] or '',
        domain=rule['domain'] or ''
    )

    # Create temporary prompt file (KT-010e: will be cleaned up)
    prompt_file = None
    try:
        # Create temp file
        fd, prompt_file = tempfile.mkstemp(suffix='.txt', text=True)
        with os.fdopen(fd, 'w') as f:
            f.write(prompt)

        # KT-010: Claude CLI invocation
        # KT-010a: Use default model (no --model flag)
        # KT-010b: Accept default temperature (no --temperature flag)
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(prompt_file),
            capture_output=True,
            text=True,
            timeout=CLASSIFICATION_TIMEOUT  # KT-010c
        )

        if result.returncode != 0:
            log_error(error_log_path, rule['id'], 'CLAUDE_CLI_ERROR',
                     f'Exit code {result.returncode}: {result.stderr}')
            return FALLBACK_WEIGHTS.copy()

        # KT-010f: Extract JSON from response (handle markdown)
        response_data = extract_json_from_markdown(result.stdout)

        if response_data is None:
            # Stage 1: JSON parse failure (KT-004a)
            log_error(error_log_path, rule['id'], 'JSON_PARSE_ERROR',
                     f'Failed to parse JSON from response')
            return FALLBACK_WEIGHTS.copy()

        # Validate weights (KT-004)
        weights = validate_weights(response_data, rule['id'], error_log_path)
        return weights

    except subprocess.TimeoutExpired:
        log_error(error_log_path, rule['id'], 'TIMEOUT',
                 f'Classification exceeded {CLASSIFICATION_TIMEOUT}s')
        return FALLBACK_WEIGHTS.copy()

    except Exception as e:
        log_error(error_log_path, rule['id'], 'CLAUDE_CLI_ERROR', str(e))
        return FALLBACK_WEIGHTS.copy()

    finally:
        # KT-010e: Prompt file cleanup (both success and failure paths)
        if prompt_file and os.path.exists(prompt_file):
            os.unlink(prompt_file)


def get_top_types(weights, n=3):
    """Format top N types with weights for progress reporting (KT-020e)"""
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    return ', '.join([f'{k}={v:.2f}' for k, v in sorted_types[:n]])


def update_rule_metadata(conn, rule_id, weights, model_id='claude-sonnet-4-5-20250929'):
    """KT-030: Update rule metadata with classification"""

    # KT-007: Classification metadata fields
    # KT-003a: Metadata structure
    classification_data = {
        'knowledge_type': weights,
        'classification_method': 'llm',
        'classification_model': model_id,
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    }

    patch_json = json.dumps(classification_data)

    # KT-030: Metadata update query
    # KT-030a: JSON merge logic with json_patch
    # KT-030c: COALESCE handles NULL metadata
    conn.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (patch_json, rule_id)
    )
    # KT-030b, KT-020d: Single UPDATE per rule (autocommit enabled)
    conn.commit()


def get_unclassified_rules(conn, limit=None):
    """KT-020a: Get unclassified rules"""
    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    # KT-020c: Optional limit flag
    if limit:
        query += f" LIMIT {limit}"

    cursor = conn.execute(query)
    return cursor.fetchall()


def classify_rules(db_path, template_path, error_log_path, limit=None):
    """KT-020: Batch classification mode"""

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get unclassified rules
    rules = get_unclassified_rules(conn, limit)
    total = len(rules)

    if total == 0:
        print("No unclassified rules found.")
        return 0

    print(f"Classifying {total} unclassified rules...")
    print()

    error_count = 0

    for idx, rule in enumerate(rules, 1):
        # Classify with Claude CLI
        weights = classify_with_claude(rule, template_path, error_log_path)

        # Check if fallback was used (indicates error)
        if weights == FALLBACK_WEIGHTS:
            error_count += 1

        # Update database (KT-020d: commit after each classification)
        update_rule_metadata(conn, rule['id'], weights)

        # KT-020e: Progress reporting
        top_types = get_top_types(weights)
        print(f"{rule['id']} ({idx}/{total}) - {top_types}")

    conn.close()

    print()

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / total) * 100
        print(f"Errors: {error_count}/{total} ({percentage:.1f}%) - see {error_log_path}")

    return error_count


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    # KT-020c: Optional limit flag
    parser = argparse.ArgumentParser(
        description='Classify rules by knowledge type using Claude CLI'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rules to classify (for testing)'
    )
    args = parser.parse_args()

    print("Context Engine - Knowledge Type Classifier")
    print("=" * 70)
    print()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Resolve paths
    db_path = PROJECT_ROOT / config['paths']['database']

    # KT-002: Prompt template source
    template_path = BASE_DIR / "templates" / "runtime-template-knowledge-classification.txt"

    # KT-040: Error log file path
    error_log_path = BASE_DIR / "data" / "classification_errors.log"

    # Verify database exists
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    # Verify template exists
    if not template_path.exists():
        print(f"Error: Template not found at {template_path}", file=sys.stderr)
        sys.exit(1)

    # KT-020: Batch classification mode
    # KT-020b: Process ALL unclassified rules by default
    error_count = classify_rules(db_path, template_path, error_log_path, limit=args.limit)

    print("Classification complete.")

    # KT-046: Non-zero exit code if any errors
    return 1 if error_count > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
