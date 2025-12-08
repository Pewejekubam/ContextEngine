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
import re
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


def load_config():
    """Load deployment configuration and vocabulary."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def get_db_path(config):
    """Get database path from config."""
    # KT-030: Database operations require absolute path
    db_relative_path = config['structure']['database_path']
    return Path(config['paths']['context_engine_home']) / db_relative_path


def get_error_log_path(config):
    """Get error log path from config."""
    # KT-040: Error log file path
    return Path(config['paths']['context_engine_home']) / 'data' / 'classification_errors.log'


def get_template_path(config):
    """Get prompt template path from config."""
    # KT-002: Prompt template source
    return Path(config['paths']['context_engine_home']) / 'templates' / 'runtime-template-knowledge-classification.txt'


def log_error(error_log_path, rule_id, error_type, details):
    """Log classification error to error log file."""
    # KT-004e, KT-041: Error logging format (TSV)
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n"

    # KT-044: Append-only, no rotation
    with open(error_log_path, 'a') as f:
        f.write(log_entry)


def normalize_weights(weights, error_log_path, rule_id):
    """Normalize weights to sum to 1.0."""
    # KT-005: Normalization algorithm
    total = sum(weights.values())

    if total == 0:
        # Cannot normalize zero sum, return None to trigger fallback
        return None

    # KT-005: normalized_weight[type] = weight[type] / sum(weights)
    normalized = {k: v / total for k, v in weights.items()}

    # KT-005c: Normalization logging
    log_entry = f"{datetime.now(timezone.utc).isoformat()}\t{rule_id}\tNORMALIZED\toriginal_sum={total} → 1.0\n"
    with open(error_log_path, 'a') as f:
        f.write(log_entry)

    return normalized


def validate_weights(weights, error_log_path, rule_id):
    """Validate weight distribution and apply normalization if needed."""
    # KT-004: Three-stage validation

    # Stage 2: Check all five keys present
    # KT-004b: Missing keys failure
    for key in KNOWLEDGE_TYPES:
        if key not in weights:
            log_error(error_log_path, rule_id, 'MISSING_KEYS', f"Missing key: {key}")
            return FALLBACK_WEIGHTS.copy()

    # Stage 3: Check weights sum within tolerance
    # KT-001a, KT-005a: sum=1.0 (tolerance: 0.95-1.05)
    total = sum(weights.values())

    if total < 0.95 or total > 1.05:
        # KT-004c: Attempt normalization
        normalized = normalize_weights(weights, error_log_path, rule_id)
        if normalized is None:
            log_error(error_log_path, rule_id, 'WEIGHT_SUM_ERROR', f"sum={total}, normalization failed")
            return FALLBACK_WEIGHTS.copy()
        return normalized

    return weights


def extract_json_from_markdown(text):
    """Extract JSON from markdown code blocks."""
    # KT-010f: Markdown response handling

    # Try to find JSON in code blocks first
    json_block_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
    matches = re.findall(json_block_pattern, text, re.DOTALL)

    if matches:
        return matches[0]

    # Try to find JSON object directly
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    matches = re.findall(json_pattern, text, re.DOTALL)

    if matches:
        # Return the first valid JSON object
        for match in matches:
            try:
                json.loads(match)
                return match
            except json.JSONDecodeError:
                continue

    return text


def classify_with_claude(rule, template_path, error_log_path):
    """Classify a rule using Claude CLI."""
    # KT-002a: Template variables
    template = template_path.read_text()
    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'] or '',
        domain=rule['domain'] or ''
    )

    # KT-010e: Create temporary prompt file
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            temp_file = f.name
            f.write(prompt)

        # KT-010: Claude CLI invocation
        # KT-010a: Use default model (claude-sonnet-4-5-20250929)
        # KT-010b: Accept default temperature
        # KT-010c: Timeout 120 seconds
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(temp_file, 'r'),
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            # KT-010d: Single attempt only, use fallback
            log_error(error_log_path, rule['id'], 'CLAUDE_CLI_ERROR',
                     f"Exit code {result.returncode}: {result.stderr}")
            return FALLBACK_WEIGHTS.copy()

        # KT-010f: Extract JSON from markdown if needed
        response_text = result.stdout.strip()
        json_text = extract_json_from_markdown(response_text)

        # Stage 1: JSON parse
        # KT-004a: JSON parse failure
        try:
            weights = json.loads(json_text)
        except json.JSONDecodeError as e:
            log_error(error_log_path, rule['id'], 'JSON_PARSE_ERROR', str(e))
            return FALLBACK_WEIGHTS.copy()

        # Validate and normalize if needed
        return validate_weights(weights, error_log_path, rule['id'])

    except subprocess.TimeoutExpired:
        # KT-042: TIMEOUT error type
        log_error(error_log_path, rule['id'], 'TIMEOUT', 'Classification exceeded 120 seconds')
        return FALLBACK_WEIGHTS.copy()
    except Exception as e:
        log_error(error_log_path, rule['id'], 'CLAUDE_CLI_ERROR', str(e))
        return FALLBACK_WEIGHTS.copy()
    finally:
        # KT-010e: Cleanup temporary file
        if temp_file and Path(temp_file).exists():
            Path(temp_file).unlink()


def get_unclassified_rules(db_path, limit=None):
    """Fetch unclassified rules from database."""
    # KT-020a: Target selection query
    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    # KT-020c: Optional limit flag
    if limit:
        query += f" LIMIT {limit}"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query)
    rules = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return rules


def update_rule_metadata(db_path, rule_id, weights, model_id='claude-sonnet-4-5-20250929'):
    """Update rule metadata with classification results."""
    # KT-003a: Metadata structure
    # KT-007: Classification metadata fields
    metadata_patch = {
        'knowledge_type': weights,
        'classification_method': 'llm',
        'classification_model': model_id,
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).isoformat()
    }

    # KT-030: Metadata update query
    # KT-030a: JSON merge logic using json_patch
    # KT-030c: Initialize metadata as '{}' if NULL
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (json.dumps(metadata_patch), rule_id)
    )

    # KT-020d: Commit after each successful classification
    conn.commit()
    conn.close()


def format_top_types(weights, top_n=3):
    """Format top N knowledge types with weights for display."""
    # KT-020e: Progress reporting format
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    top_types = sorted_types[:top_n]
    return ', '.join([f"{t}:{w:.2f}" for t, w in top_types])


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Classify rules by knowledge type using Claude CLI'
    )
    # KT-020c: Optional limit flag
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rules to classify (for testing)'
    )
    args = parser.parse_args()

    print("Context Engine - Knowledge Type Classifier v1.1.0")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get paths
    db_path = get_db_path(config)
    error_log_path = get_error_log_path(config)
    template_path = get_template_path(config)

    # Verify paths exist
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    if not template_path.exists():
        print(f"Error: Template not found at {template_path}", file=sys.stderr)
        sys.exit(1)

    # Ensure error log directory exists
    error_log_path.parent.mkdir(parents=True, exist_ok=True)

    # KT-020: Batch classification mode
    # KT-020b: Process ALL unclassified rules by default
    rules = get_unclassified_rules(db_path, limit=args.limit)

    if not rules:
        print("\nNo unclassified rules found.")
        return 0

    total = len(rules)
    print(f"\nFound {total} unclassified rule(s)")
    print()

    error_count = 0

    # Process each rule
    for idx, rule in enumerate(rules, 1):
        # Classify using Claude
        weights = classify_with_claude(rule, template_path, error_log_path)

        # Check if fallback was used (indicates error)
        if weights == FALLBACK_WEIGHTS:
            error_count += 1

        # Update database
        update_rule_metadata(db_path, rule['id'], weights)

        # KT-020e: Progress reporting
        top_types = format_top_types(weights)
        print(f"{rule['id']} ({idx}/{total}) - {top_types}")

    print()

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / total) * 100
        print(f"Errors: {error_count}/{total} ({percentage:.1f}%) - see {error_log_path}")
        print()
        # KT-046: Non-zero exit code if any errors
        return 1
    else:
        print("Classification complete. No errors.")
        print()
        # KT-046: Exit 0 if all successful
        return 0


if __name__ == '__main__':
    sys.exit(main())
