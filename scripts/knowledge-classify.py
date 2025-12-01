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
import argparse
import tempfile
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

# KT-010c: Timeout for classification
CLAUDE_TIMEOUT = 120


def load_config():
    """Load deployment configuration and vocabulary."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def get_error_log_path():
    """KT-040: Error log file path"""
    data_dir = BASE_DIR / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir / "classification_errors.log"


def log_error(rule_id: str, error_type: str, details: str):
    """KT-041: Error logging format (TSV)"""
    timestamp = datetime.now(timezone.utc).isoformat()
    error_log = get_error_log_path()
    with open(error_log, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n")


def log_normalization(rule_id: str, original_sum: float):
    """KT-005c: Normalization logging"""
    timestamp = datetime.now(timezone.utc).isoformat()
    error_log = get_error_log_path()
    with open(error_log, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\tNORMALIZED\toriginal_sum={original_sum:.4f} → 1.0\n")


def normalize_weights(weights: dict, rule_id: str) -> dict:
    """KT-005: Normalization algorithm"""
    total = sum(weights.values())

    # KT-005a: Normalization trigger
    if total < 0.95 or total > 1.05:
        # KT-005c: Log normalization
        log_normalization(rule_id, total)

        # Normalize
        if total > 0:
            normalized = {k: v / total for k, v in weights.items()}
        else:
            # All zeros - use fallback
            return None

        # KT-005b: Verify sum is close to 1.0
        new_sum = sum(normalized.values())
        if 0.9999 <= new_sum <= 1.0001:
            return normalized
        else:
            return None

    return weights


def validate_classification(response: dict, rule_id: str) -> tuple[dict, bool]:
    """
    KT-004: Three-stage validation
    Returns: (weights_dict, success_flag)
    """
    # Stage 1: JSON parse already done by caller

    # Stage 2: KT-004b - All five keys present
    missing_keys = set(KNOWLEDGE_TYPES) - set(response.keys())
    if missing_keys:
        error_msg = f"Missing keys: {missing_keys}"
        log_error(rule_id, "MISSING_KEYS", error_msg)
        return FALLBACK_WEIGHTS, False

    # Extract weights
    weights = {k: response[k] for k in KNOWLEDGE_TYPES}

    # Stage 3: KT-004c - Weight sum within tolerance
    total = sum(weights.values())
    if total < 0.95 or total > 1.05:
        # Attempt normalization
        normalized = normalize_weights(weights, rule_id)
        if normalized:
            return normalized, True
        else:
            # KT-004c: Normalization failed, use fallback
            error_msg = f"Weight sum {total:.4f} out of range, normalization failed"
            log_error(rule_id, "WEIGHT_SUM_ERROR", error_msg)
            return FALLBACK_WEIGHTS, False

    return weights, True


def extract_json_from_markdown(text: str) -> str:
    """KT-010f: Extract JSON from markdown code blocks"""
    # Try to find JSON in code blocks
    json_block_pattern = r'```(?:json)?\s*(\{[^`]+\})\s*```'
    match = re.search(json_block_pattern, text, re.DOTALL)
    if match:
        return match.group(1)

    # Try to find raw JSON object
    json_pattern = r'(\{[^{}]*\})'
    match = re.search(json_pattern, text, re.DOTALL)
    if match:
        return match.group(1)

    return text


def load_classification_template():
    """KT-002: Load prompt template"""
    template_path = BASE_DIR / "templates" / "runtime-template-knowledge-classification.txt"
    with open(template_path, 'r') as f:
        return f.read()


def classify_rule_with_claude(rule: dict) -> tuple[dict, bool, str]:
    """
    KT-010: Claude CLI invocation
    Returns: (weights_dict, success_flag, model_id)
    """
    rule_id = rule['id']

    # Load template and substitute variables (KT-002a)
    template = load_classification_template()
    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'] or '',
        domain=rule['domain'] or ''
    )

    # Create temporary prompt file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
        tmp.write(prompt)
        prompt_file = tmp.name

    try:
        # KT-010: Claude CLI invocation with --print flag
        # KT-010c: 120 second timeout
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(prompt_file, 'r'),
            capture_output=True,
            text=True,
            timeout=CLAUDE_TIMEOUT
        )

        if result.returncode != 0:
            error_msg = f"Claude CLI failed: {result.stderr}"
            log_error(rule_id, "CLAUDE_CLI_ERROR", error_msg)
            return FALLBACK_WEIGHTS, False, None

        response_text = result.stdout.strip()

        # KT-010f: Extract JSON from markdown if needed
        json_text = extract_json_from_markdown(response_text)

        # KT-004a: Parse JSON
        try:
            response_data = json.loads(json_text)
        except json.JSONDecodeError as e:
            error_msg = f"JSON parse error: {e}"
            log_error(rule_id, "JSON_PARSE_ERROR", error_msg)
            return FALLBACK_WEIGHTS, False, None

        # Validate classification
        weights, success = validate_classification(response_data, rule_id)

        # KT-010a: Default model (we assume claude-sonnet-4-5-20250929)
        model_id = "claude-sonnet-4-5-20250929"

        return weights, success, model_id

    except subprocess.TimeoutExpired:
        log_error(rule_id, "TIMEOUT", f"Classification exceeded {CLAUDE_TIMEOUT}s timeout")
        return FALLBACK_WEIGHTS, False, None
    except Exception as e:
        log_error(rule_id, "CLAUDE_CLI_ERROR", str(e))
        return FALLBACK_WEIGHTS, False, None
    finally:
        # KT-010e: Cleanup prompt file
        try:
            Path(prompt_file).unlink()
        except:
            pass


def update_rule_metadata(conn: sqlite3.Connection, rule_id: str, weights: dict,
                        model_id: str, success: bool):
    """KT-030: Update rule metadata with classification"""
    # KT-003a: Metadata structure
    # KT-007: Classification metadata fields
    metadata_patch = {
        'knowledge_type': weights,
        'classification_method': 'llm',
        'classification_model': model_id,
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    # KT-030: Use json_patch to merge with existing metadata
    # KT-030c: Handle NULL metadata
    patch_json = json.dumps(metadata_patch)

    conn.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (patch_json, rule_id)
    )
    # KT-020d: Commit after each classification (autocommit)
    conn.commit()


def get_unclassified_rules(conn: sqlite3.Connection, limit: int = None) -> list:
    """KT-020a: Target selection query"""
    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    if limit:
        query += f" LIMIT {limit}"

    cursor = conn.execute(query)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def format_top_types(weights: dict, top_n: int = 3) -> str:
    """KT-020e: Format top N types with weights for progress reporting"""
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    top_types = sorted_types[:top_n]
    return ', '.join([f"{k}={v:.2f}" for k, v in top_types])


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    parser = argparse.ArgumentParser(
        description='Classify rules into knowledge types using Claude CLI'
    )
    # KT-020c: Optional limit flag
    parser.add_argument('--limit', type=int, help='Limit number of rules to classify')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    args = parser.parse_args()

    print("Context Engine - Knowledge Type Classifier")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1

    # Connect to database
    db_path = PROJECT_ROOT / config['database']['path']
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(db_path))

    try:
        # KT-020: Batch classification mode
        rules = get_unclassified_rules(conn, limit=args.limit)
        total = len(rules)

        if total == 0:
            print("\nNo unclassified rules found.")
            return 0

        print(f"\nFound {total} unclassified rule{'s' if total != 1 else ''}")
        print(f"Classification template: {PROMPT_VERSION}")
        print(f"Timeout per rule: {CLAUDE_TIMEOUT}s")
        print()

        # KT-043: Track errors for final report
        error_count = 0

        # Process each rule
        for idx, rule in enumerate(rules, 1):
            rule_id = rule['id']

            # Classify using Claude
            weights, success, model_id = classify_rule_with_claude(rule)

            if not success:
                error_count += 1

            # Update database (even on failure, we store fallback)
            update_rule_metadata(conn, rule_id, weights, model_id, success)

            # KT-020e: Progress reporting
            top_types = format_top_types(weights)
            status = "✓" if success else "✗ (fallback)"
            print(f"[{idx}/{total}] {rule_id} {status} - {top_types}")

            if args.verbose and not success:
                print(f"         Using fallback weights")

        print()

        # KT-045: Error summary report
        if error_count > 0:
            percentage = (error_count / total) * 100
            error_log = get_error_log_path()
            print(f"Errors: {error_count}/{total} ({percentage:.1f}%) - see {error_log}")
            print()
            # KT-046: Non-zero exit code if any errors
            return 1
        else:
            print(f"Successfully classified {total} rule{'s' if total != 1 else ''}")
            print()
            return 0

    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
