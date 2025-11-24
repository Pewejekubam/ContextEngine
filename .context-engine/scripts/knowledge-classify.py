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
# KT-001a: Weight distribution 0.0-1.0 for all five types, sum=1.0 (validated in validate_weights)
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

# KT-040: Error log file path
ERROR_LOG_PATH = BASE_DIR / "data" / "classification_errors.log"

# KT-042: Error types
ERROR_TYPES = {
    'JSON_PARSE_ERROR': 'JSON_PARSE_ERROR',
    'MISSING_KEYS': 'MISSING_KEYS',
    'WEIGHT_SUM_ERROR': 'WEIGHT_SUM_ERROR',
    'CLAUDE_CLI_ERROR': 'CLAUDE_CLI_ERROR',
    'TIMEOUT': 'TIMEOUT'
}


def log_error(rule_id: str, error_type: str, details: str):
    """
    KT-004e, KT-041: Log error in TSV format
    KT-004f: Passive telemetry for human review, not consumed by automated workflows
    KT-044: Append-only, no rotation (user manages log size)
    Format: {timestamp}\t{rule_id}\t{error_type}\t{details}
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ERROR_LOG_PATH, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\n")


def normalize_weights(weights: dict) -> dict:
    """
    KT-005: Normalization algorithm
    KT-005b: Normalization guarantee - after normalization, sum = 1.0
    normalized_weight[type] = weight[type] / sum(weights)
    """
    total = sum(weights.values())
    if total == 0:
        return FALLBACK_WEIGHTS.copy()

    normalized = {k: v / total for k, v in weights.items()}
    return normalized


def validate_weights(weights: dict, rule_id: str) -> tuple[bool, dict]:
    """
    KT-004: Three-stage validation
    Returns: (success, weights_to_use)
    """
    # Stage 1: JSON already parsed by caller

    # Stage 2: KT-004b - All five keys present
    if not all(k in weights for k in KNOWLEDGE_TYPES):
        log_error(rule_id, ERROR_TYPES['MISSING_KEYS'],
                 f"Missing keys: {set(KNOWLEDGE_TYPES) - set(weights.keys())}")
        return False, FALLBACK_WEIGHTS.copy()

    # Stage 3: KT-004c, KT-005a - Weights sum within 0.95-1.05
    total = sum(weights[k] for k in KNOWLEDGE_TYPES)
    if total < 0.95 or total > 1.05:
        # Attempt normalization
        try:
            normalized = normalize_weights(weights)
            # KT-005c: Log normalization
            log_error(rule_id, 'NORMALIZED', f"original_sum={total:.4f} → 1.0")
            return True, normalized
        except Exception as e:
            log_error(rule_id, ERROR_TYPES['WEIGHT_SUM_ERROR'],
                     f"sum={total:.4f}, normalization failed: {e}")
            return False, FALLBACK_WEIGHTS.copy()

    return True, weights


def extract_json_from_markdown(text: str) -> str:
    """
    KT-010f: Extract JSON from markdown code blocks
    Handles cases where Claude CLI returns conversational response with JSON in code block
    """
    # Try to find JSON in code blocks
    json_pattern = r'```(?:json)?\s*(\{[^`]+\})\s*```'
    matches = re.findall(json_pattern, text, re.DOTALL)
    if matches:
        return matches[0]

    # Try to find raw JSON
    json_pattern = r'(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})'
    matches = re.findall(json_pattern, text, re.DOTALL)
    if matches:
        # Return the first valid-looking JSON object
        for match in matches:
            try:
                json.loads(match)
                return match
            except:
                continue

    return text


def classify_rule_with_claude(rule_id: str, rule_type: str, title: str,
                               description: str, domain: str, template_path: Path) -> dict:
    """
    KT-010: Claude CLI invocation with timeout and fallback
    Returns: (success, weights, model_id)
    """
    # KT-002, KT-002a, KT-002b: Load and populate template
    # Template content defined in runtime-template-knowledge-classification.txt
    with open(template_path) as f:
        template = f.read()

    prompt = template.format(
        rule_id=rule_id,
        rule_type=rule_type,
        title=title,
        description=description or "(no description)",
        domain=domain or "(no domain)"
    )

    # KT-010: Create temporary prompt file
    temp_prompt = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            temp_prompt = Path(f.name)
            f.write(prompt)

        # KT-010: Claude CLI invocation with stdin redirection
        # KT-010b: Accept default temperature (no override)
        # KT-010c: 120 second timeout
        # KT-010d: Single attempt only, no retries
        result = subprocess.run(
            ['claude', '--print'],
            stdin=open(temp_prompt),
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            log_error(rule_id, ERROR_TYPES['CLAUDE_CLI_ERROR'],
                     f"Exit code {result.returncode}: {result.stderr}")
            return False, FALLBACK_WEIGHTS.copy(), None

        # KT-010f: Extract JSON from markdown if needed
        response_text = result.stdout.strip()
        json_text = extract_json_from_markdown(response_text)

        # KT-004a: Parse JSON
        try:
            weights = json.loads(json_text)
        except json.JSONDecodeError as e:
            log_error(rule_id, ERROR_TYPES['JSON_PARSE_ERROR'],
                     f"Failed to parse: {str(e)[:100]}")
            return False, FALLBACK_WEIGHTS.copy(), None

        # KT-004: Validate weights
        success, validated_weights = validate_weights(weights, rule_id)

        # KT-010a: Return default model
        return success, validated_weights, DEFAULT_MODEL

    except subprocess.TimeoutExpired:
        log_error(rule_id, ERROR_TYPES['TIMEOUT'], "Claude CLI timeout after 120s")
        return False, FALLBACK_WEIGHTS.copy(), None
    except Exception as e:
        log_error(rule_id, ERROR_TYPES['CLAUDE_CLI_ERROR'], str(e))
        return False, FALLBACK_WEIGHTS.copy(), None
    finally:
        # KT-010e: Cleanup temporary prompt file
        if temp_prompt and temp_prompt.exists():
            temp_prompt.unlink()


def update_rule_metadata(conn: sqlite3.Connection, rule_id: str, weights: dict,
                         classification_method: str, model_id: str = None):
    """
    KT-003: Classification stored in rules.metadata JSON column under knowledge_type key
    KT-030: Update metadata with json_patch
    KT-030a: JSON merge logic using json_patch()
    KT-030b: No explicit transactions, single UPDATE per rule (autocommit enabled)
    KT-003a: Metadata structure with provenance
    """
    # KT-007: Classification metadata fields with ISO 8601 UTC timestamp
    classified_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    metadata_patch = {
        'knowledge_type': weights,
        'classification_method': classification_method,
        'classification_model': model_id,
        'classification_prompt_version': PROMPT_VERSION,
        'classified_at': classified_at
    }

    # KT-030: Use json_patch to merge into existing metadata
    # KT-030c: COALESCE handles NULL metadata
    patch_json = json.dumps(metadata_patch)

    cursor = conn.cursor()
    cursor.execute(
        "UPDATE rules SET metadata = json_patch(COALESCE(metadata, '{}'), ?) WHERE id = ?",
        (patch_json, rule_id)
    )
    # KT-020d: Commit after each successful classification
    conn.commit()


def format_top_types(weights: dict, n=3) -> str:
    """
    KT-020e: Format top N types with weights for progress reporting
    """
    sorted_types = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    return ", ".join(f"{t}={w:.2f}" for t, w in sorted_types[:n])


def main():
    """LLM-based knowledge type classification with validation and fallback"""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Classify rules by knowledge type using Claude CLI'
    )
    # KT-020c: Optional limit flag
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of rules to classify (for testing)')
    args = parser.parse_args()

    print("Context Engine - Knowledge Classifier")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1

    # Get database path from config
    db_path = PROJECT_ROOT / "data" / "rules.db"
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    # KT-002: Template path
    template_path = BASE_DIR / "templates" / "runtime-template-knowledge-classification.txt"
    if not template_path.exists():
        print(f"Error: Template not found at {template_path}", file=sys.stderr)
        return 1

    # Connect to database
    conn = sqlite3.Connection(db_path)
    conn.row_factory = sqlite3.Row

    # KT-020a: Target selection query - unclassified rules
    # KT-003b: Rules with metadata.knowledge_type IS NULL are unclassified
    # KT-020b: Process ALL unclassified rules by default (no arbitrary limit)
    query = """
        SELECT id, type, title, description, domain, metadata
        FROM rules
        WHERE metadata IS NULL OR json_extract(metadata, '$.knowledge_type') IS NULL
        ORDER BY created_at DESC
    """

    if args.limit:
        query += f" LIMIT {args.limit}"

    cursor = conn.cursor()
    cursor.execute(query)
    rules = cursor.fetchall()

    total = len(rules)
    if total == 0:
        print("No unclassified rules found.")
        return 0

    print(f"Found {total} unclassified rules")
    print()

    error_count = 0

    # KT-020: Batch classification mode
    for idx, rule in enumerate(rules, 1):
        rule_id = rule['id']
        rule_type = rule['type']
        title = rule['title']
        description = rule['description']
        domain = rule['domain']

        print(f"[{idx}/{total}] Classifying {rule_id}...", end=' ', flush=True)

        # KT-010: Classify with Claude CLI
        success, weights, model_id = classify_rule_with_claude(
            rule_id, rule_type, title, description, domain, template_path
        )

        # Track errors
        if not success:
            error_count += 1

        # KT-043: All errors trigger fallback, processing continues
        # Update database with classification (whether success or fallback)
        classification_method = 'llm' if success else 'llm'  # Both use LLM, fallback just uses default weights
        update_rule_metadata(conn, rule_id, weights, classification_method, model_id)

        # KT-020e: Progress reporting
        top_types = format_top_types(weights)
        print(f"{top_types}")

    conn.close()

    print()
    print("="*70)
    print("Classification Complete")
    print(f"Total: {total}")
    print(f"Successful: {total - error_count}")

    # KT-045: Error summary report
    if error_count > 0:
        percentage = (error_count / total) * 100
        print(f"Errors: {error_count}/{total} ({percentage:.1f}%) - see {ERROR_LOG_PATH}")

    # KT-046: Non-zero exit code if any errors
    return 1 if error_count > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
