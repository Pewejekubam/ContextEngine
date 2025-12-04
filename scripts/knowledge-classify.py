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

# KT-010c: Timeout for Claude CLI
CLAUDE_TIMEOUT = 120


def log_error(error_log_path: Path, rule_id: str, error_type: str, details: str):
    """
    KT-004e, KT-041: Log error to classification_errors.log in TSV format
    Format: {timestamp}\t{rule_id}\t{error_type}\t{details}
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    log_line = f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n"

    # KT-044: Append-only, no rotation
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(error_log_path, 'a') as f:
        f.write(log_line)


def normalize_weights(weights: dict, rule_id: str, error_log_path: Path) -> dict:
    """
    KT-005: Normalization algorithm
    KT-005a: Trigger on sum < 0.95 or sum > 1.05
    KT-005b: Guarantee sum = 1.0 after normalization
    KT-005c: Log normalization events
    """
    total = sum(weights.values())

    # KT-005a: Check if normalization needed
    if total < 0.95 or total > 1.05:
        original_sum = total

        # KT-005: Normalize
        normalized = {k: v / total for k, v in weights.items()}

        # KT-005c: Log normalization
        log_error(error_log_path, rule_id, "NORMALIZED",
                 f"original_sum={original_sum:.4f} → 1.0")

        return normalized

    return weights


def validate_weights(weights: dict, rule_id: str, error_log_path: Path) -> tuple[bool, dict]:
    """
    KT-004: Three-stage validation
    Returns: (is_valid, validated_weights)
    """
    # Stage 1: JSON parse success (handled by caller)

    # Stage 2: KT-004b - All five keys present
    missing_keys = [k for k in KNOWLEDGE_TYPES if k not in weights]
    if missing_keys:
        log_error(error_log_path, rule_id, "MISSING_KEYS",
                 f"Missing keys: {', '.join(missing_keys)}")
        return False, FALLBACK_WEIGHTS

    # Stage 3: KT-004c - Weights sum within 0.95-1.05
    total = sum(weights.values())
    if total < 0.95 or total > 1.05:
        # Attempt normalization
        try:
            normalized = normalize_weights(weights, rule_id, error_log_path)
            return True, normalized
        except Exception as e:
            log_error(error_log_path, rule_id, "WEIGHT_SUM_ERROR",
                     f"sum={total:.4f}, normalization failed: {e}")
            return False, FALLBACK_WEIGHTS

    return True, weights


def extract_json_from_response(response: str) -> str:
    """
    KT-010f: Extract JSON from markdown code blocks if Claude CLI returns conversational response
    """
    # Try to parse as-is first
    try:
        json.loads(response)
        return response
    except json.JSONDecodeError:
        pass

    # Look for JSON in markdown code blocks
    json_pattern = r'```(?:json)?\s*(\{[^`]+\})\s*```'
    matches = re.findall(json_pattern, response, re.DOTALL)
    if matches:
        return matches[0].strip()

    # Look for standalone JSON object
    json_pattern = r'\{[^}]+\}'
    matches = re.findall(json_pattern, response, re.DOTALL)
    if matches:
        # Try each match to see if it's valid JSON
        for match in matches:
            try:
                json.loads(match)
                return match
            except json.JSONDecodeError:
                continue

    return response


def classify_with_claude(rule: dict, template_path: Path, error_log_path: Path) -> tuple[dict, str]:
    """
    KT-010: Classify rule using Claude CLI
    Returns: (weights_dict, model_id)
    """
    rule_id = rule['id']

    # KT-002a: Load and fill template variables
    with open(template_path) as f:
        template = f.read()

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
        prompt_file = Path(tmp.name)

    try:
        # KT-010: Claude CLI invocation with timeout
        # KT-010a: Use default model
        # KT-010b: Accept default temperature
        with open(prompt_file, 'r') as f:
            result = subprocess.run(
                ['claude', '--print'],
                stdin=f,
                capture_output=True,
                text=True,
                timeout=CLAUDE_TIMEOUT
            )

        if result.returncode != 0:
            log_error(error_log_path, rule_id, "CLAUDE_CLI_ERROR",
                     f"Exit code {result.returncode}: {result.stderr}")
            return FALLBACK_WEIGHTS, None

        # KT-010f: Extract JSON from response
        response = extract_json_from_response(result.stdout.strip())

        # KT-004: Parse JSON
        try:
            weights = json.loads(response)
        except json.JSONDecodeError as e:
            # KT-004a: JSON parse failure
            log_error(error_log_path, rule_id, "JSON_PARSE_ERROR",
                     f"Failed to parse: {e}")
            return FALLBACK_WEIGHTS, None

        # Validate weights
        is_valid, validated_weights = validate_weights(weights, rule_id, error_log_path)

        # KT-010a: Model ID is default Claude CLI model
        model_id = 'claude-sonnet-4-5-20250929'

        return validated_weights, model_id

    except subprocess.TimeoutExpired:
        # KT-010c: Timeout handling
        log_error(error_log_path, rule_id, "TIMEOUT",
                 f"Claude CLI timeout after {CLAUDE_TIMEOUT}s")
        return FALLBACK_WEIGHTS, None

    except Exception as e:
        log_error(error_log_path, rule_id, "CLAUDE_CLI_ERROR", str(e))
        return FALLBACK_WEIGHTS, None

    finally:
        # KT-010e: Cleanup prompt file
        if prompt_file.exists():
            prompt_file.unlink()


def update_rule_metadata(db_path: Path, rule_id: str, weights: dict, model_id: str or None):
    """
    KT-030: Update rules.metadata with classification results
    KT-003a: Store metadata structure with provenance
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # KT-007: Classification metadata fields
    # KT-003a: Metadata structure
    classification_data = {
        'knowledge_type': weights,
        'classification_method': 'llm',
        'classification_model': model_id,
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    # KT-030: Use json_patch to merge with existing metadata
    # KT-030c: Handle NULL metadata
    patch = json.dumps(classification_data)

    cursor.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (patch, rule_id)
    )

    # KT-020d, KT-030b: Commit after each rule (autocommit)
    conn.commit()
    conn.close()


def get_top_types(weights: dict, n: int = 3) -> str:
    """
    KT-020e: Format top N types with weights for progress reporting
    """
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    top_n = sorted_types[:n]
    return ', '.join([f"{t}={w:.2f}" for t, w in top_n])


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Knowledge type classifier with Claude CLI integration'
    )
    parser.add_argument('--limit', type=int, default=None,
                       help='KT-020c: Limit number of rules to process (for testing)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show rules to be classified without processing')

    args = parser.parse_args()

    print("Context Engine - Knowledge Type Classifier v1.1.0")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get paths from config
    db_path = PROJECT_ROOT / config['paths']['database']
    template_path = BASE_DIR / "templates" / "runtime-template-knowledge-classification.txt"
    error_log_path = BASE_DIR / "data" / "classification_errors.log"

    # Verify database exists
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    # Verify template exists
    if not template_path.exists():
        print(f"Error: Template not found at {template_path}", file=sys.stderr)
        sys.exit(1)

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

    # KT-020c: Apply limit if specified
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

    if args.dry_run:
        print("\nDry run - rules to be classified:")
        for i, rule in enumerate(rules, 1):
            print(f"  {i}. {rule['id']} - {rule['title']}")
        return 0

    print("\nClassifying rules...")
    print()

    error_count = 0

    # KT-020: Batch classification
    for i, rule in enumerate(rules, 1):
        rule_id = rule['id']

        # Classify with Claude
        weights, model_id = classify_with_claude(rule, template_path, error_log_path)

        # Track if this was a fallback (error occurred)
        if model_id is None:
            error_count += 1

        # Update database
        update_rule_metadata(db_path, rule_id, weights, model_id)

        # KT-020e: Progress reporting
        top_types = get_top_types(weights)
        print(f"  {rule_id} ({i}/{total}) - {top_types}")

    print()
    print(f"Classification complete: {total} rule(s) processed")

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / total) * 100
        print(f"\nErrors: {error_count}/{total} ({percentage:.1f}%) - see {error_log_path}")
        # KT-046: Non-zero exit code if any errors
        return 1

    # KT-046: Exit 0 if all successful
    return 0


if __name__ == '__main__':
    sys.exit(main())
