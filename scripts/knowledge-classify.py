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
import re
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
PROMPT_VERSION = "v1.0.0"

# KT-010c: Timeout per classification (seconds)
CLAUDE_TIMEOUT = 120

# KT-042: Error types
ERROR_TYPES = {
    'JSON_PARSE_ERROR': 'JSON_PARSE_ERROR',
    'MISSING_KEYS': 'MISSING_KEYS',
    'WEIGHT_SUM_ERROR': 'WEIGHT_SUM_ERROR',
    'CLAUDE_CLI_ERROR': 'CLAUDE_CLI_ERROR',
    'TIMEOUT': 'TIMEOUT'
}


def log_error(error_log_path: Path, rule_id: str, error_type: str, details: str):
    """KT-041: Log error in TSV format"""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n")


def log_normalization(error_log_path: Path, rule_id: str, original_sum: float):
    """KT-005c: Log normalization event"""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\tNORMALIZED\toriginal_sum={original_sum:.4f} → 1.0\n")


def normalize_weights(weights: dict) -> dict:
    """KT-005: Normalize weights to sum to 1.0"""
    total = sum(weights.values())
    if total == 0:
        return FALLBACK_WEIGHTS.copy()

    normalized = {k: v / total for k, v in weights.items()}
    return normalized


def validate_weights(weights: dict, rule_id: str, error_log_path: Path) -> tuple[dict, bool]:
    """
    KT-004: Three-stage validation
    Returns: (final_weights, had_error)
    """
    # Stage 1: Already validated by JSON parsing in caller

    # Stage 2: KT-004b - Check all five keys present
    missing_keys = set(KNOWLEDGE_TYPES) - set(weights.keys())
    if missing_keys:
        log_error(error_log_path, rule_id, ERROR_TYPES['MISSING_KEYS'],
                  f"Missing keys: {', '.join(missing_keys)}")
        return FALLBACK_WEIGHTS.copy(), True

    # Stage 3: KT-004c, KT-005a - Check weight sum
    weight_sum = sum(weights.values())
    if weight_sum < 0.95 or weight_sum > 1.05:
        # Attempt normalization
        original_sum = weight_sum
        try:
            normalized = normalize_weights(weights)
            log_normalization(error_log_path, rule_id, original_sum)
            return normalized, False
        except Exception as e:
            log_error(error_log_path, rule_id, ERROR_TYPES['WEIGHT_SUM_ERROR'],
                      f"sum={weight_sum:.4f}, normalization failed: {str(e)}")
            return FALLBACK_WEIGHTS.copy(), True

    return weights, False


def extract_json_from_markdown(text: str) -> str:
    """KT-010f: Extract JSON from markdown code blocks"""
    # Try to find JSON in code blocks
    json_block_pattern = r'```(?:json)?\s*(\{[^`]+\})\s*```'
    match = re.search(json_block_pattern, text, re.DOTALL)
    if match:
        return match.group(1)

    # Try to find raw JSON object
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    match = re.search(json_pattern, text, re.DOTALL)
    if match:
        return match.group(0)

    return text


def classify_with_claude(rule: dict, template_path: Path, error_log_path: Path) -> tuple[dict, bool]:
    """
    KT-010: Classify rule using Claude CLI
    Returns: (weights, had_error)
    """
    rule_id = rule['id']

    # KT-002a: Load and substitute template variables
    try:
        with open(template_path) as f:
            prompt_template = f.read()
    except Exception as e:
        log_error(error_log_path, rule_id, ERROR_TYPES['CLAUDE_CLI_ERROR'],
                  f"Failed to load template: {str(e)}")
        return FALLBACK_WEIGHTS.copy(), True

    # Substitute variables
    prompt = prompt_template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'] or '',
        domain=rule['domain'] or ''
    )

    # KT-010: Create temporary prompt file and invoke Claude CLI
    temp_prompt = None
    try:
        # Create temp file
        temp_prompt = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
        temp_prompt.write(prompt)
        temp_prompt.close()

        # KT-010: Invoke Claude CLI with timeout
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(temp_prompt.name),
            capture_output=True,
            text=True,
            timeout=CLAUDE_TIMEOUT
        )

        if result.returncode != 0:
            log_error(error_log_path, rule_id, ERROR_TYPES['CLAUDE_CLI_ERROR'],
                      f"Exit code {result.returncode}: {result.stderr[:200]}")
            return FALLBACK_WEIGHTS.copy(), True

        # KT-010f: Extract JSON from response (may be wrapped in markdown)
        response_text = result.stdout.strip()
        json_text = extract_json_from_markdown(response_text)

        # KT-004a: Parse JSON
        try:
            weights = json.loads(json_text)
        except json.JSONDecodeError as e:
            log_error(error_log_path, rule_id, ERROR_TYPES['JSON_PARSE_ERROR'],
                      f"JSON decode failed: {str(e)}, response: {response_text[:200]}")
            return FALLBACK_WEIGHTS.copy(), True

        # KT-004: Validate weights
        validated_weights, had_error = validate_weights(weights, rule_id, error_log_path)
        return validated_weights, had_error

    except subprocess.TimeoutExpired:
        log_error(error_log_path, rule_id, ERROR_TYPES['TIMEOUT'],
                  f"Classification timed out after {CLAUDE_TIMEOUT}s")
        return FALLBACK_WEIGHTS.copy(), True

    except Exception as e:
        log_error(error_log_path, rule_id, ERROR_TYPES['CLAUDE_CLI_ERROR'],
                  f"Unexpected error: {str(e)}")
        return FALLBACK_WEIGHTS.copy(), True

    finally:
        # KT-010e: Cleanup temporary prompt file
        if temp_prompt and Path(temp_prompt.name).exists():
            Path(temp_prompt.name).unlink()


def get_top_types(weights: dict, n: int = 3) -> str:
    """KT-020e: Format top N types with weights for progress reporting"""
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    top_n = sorted_types[:n]
    return ', '.join([f"{t}={w:.2f}" for t, w in top_n])


def update_rule_metadata(db_path: Path, rule_id: str, weights: dict, model_id: str = None):
    """
    KT-030: Update rule metadata with classification
    KT-003a: Metadata structure with provenance tracking
    """
    # KT-007: Classification metadata fields
    classification_data = {
        'knowledge_type': weights,
        'classification_method': 'llm',
        'classification_model': model_id or 'claude-sonnet-4-5-20250929',
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    conn = sqlite3.connect(db_path)
    try:
        # KT-030: Use json_patch to merge into existing metadata
        patch_json = json.dumps(classification_data)
        conn.execute(
            "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
            (patch_json, rule_id)
        )
        conn.commit()
    finally:
        conn.close()


def get_unclassified_rules(db_path: Path, limit: int = None) -> list[dict]:
    """KT-020a: Select unclassified rules"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    if limit:
        query += f" LIMIT {limit}"

    try:
        cursor = conn.execute(query)
        rules = [dict(row) for row in cursor.fetchall()]
        return rules
    finally:
        conn.close()


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    parser = argparse.ArgumentParser(
        description='Context Engine - Knowledge Type Classifier'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rules to classify (for testing)'
    )
    args = parser.parse_args()

    print("Context Engine - Knowledge Type Classifier")
    print("=" * 70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Resolve paths
    db_path = PROJECT_ROOT / config['database']['path']
    template_path = BASE_DIR / "templates" / "runtime-template-knowledge-classification.txt"
    error_log_path = BASE_DIR / "data" / "classification_errors.log"

    # Verify database exists
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    # Verify template exists
    if not template_path.exists():
        print(f"Error: Classification template not found at {template_path}", file=sys.stderr)
        sys.exit(1)

    # KT-020a: Get unclassified rules
    print("\nFetching unclassified rules...")
    rules = get_unclassified_rules(db_path, limit=args.limit)
    total = len(rules)

    if total == 0:
        print("No unclassified rules found.")
        return 0

    print(f"Found {total} unclassified rule(s)")

    if args.limit:
        print(f"Processing limit: {args.limit}")

    print()

    # KT-020: Batch classification
    error_count = 0

    for idx, rule in enumerate(rules, start=1):
        rule_id = rule['id']

        # KT-010: Classify with Claude CLI
        weights, had_error = classify_with_claude(rule, template_path, error_log_path)

        if had_error:
            error_count += 1

        # KT-030d: Commit after each classification
        update_rule_metadata(db_path, rule_id, weights)

        # KT-020e: Progress reporting
        top_types = get_top_types(weights)
        status = "[ERROR - FALLBACK]" if had_error else "[OK]"
        print(f"{status} {rule_id} ({idx}/{total}) - {top_types}")

    print()
    print("=" * 70)
    print(f"Classification complete: {total} rules processed")

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / total) * 100
        print(f"Errors: {error_count}/{total} ({percentage:.1f}%) - see {error_log_path}")
        # KT-046: Non-zero exit if any errors
        return 1
    else:
        print("All classifications successful (no fallbacks used)")
        return 0


if __name__ == '__main__':
    sys.exit(main())
