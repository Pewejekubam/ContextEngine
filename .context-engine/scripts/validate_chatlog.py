#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-001 through CAP-073
Generated from: specs/modules/runtime-command-chatlog-capture-v1.10.0.yaml
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
    """Load deployment configuration and vocabulary (CAP-066)."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    # CAP-066: Load vocabulary file from deployment config for domain validation
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # CAP-067: Extract domains with hyphen->underscore normalization for Python compatibility
    # Handle both dictionary format (Spec 23 v1.1.0+) and list format (legacy)
    tier_1_domains = vocabulary['tier_1_domains']
    if isinstance(tier_1_domains, dict):
        # Dictionary format: keys are domain names
        config['domain_tags'] = [
            d.replace('-', '_') for d in tier_1_domains.keys()
        ]
    else:
        # List format (legacy)
        config['domain_tags'] = [
            d.replace('-', '_') for d in tier_1_domains
        ]

    return config


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_uuid(uuid_str):
    """Validate UUID v4 format (CAP-040b)."""
    import re
    # UUID v4 pattern: 8-4-4-4-12 hex digits
    pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    return bool(re.match(pattern, str(uuid_str).lower()))


def validate_iso8601_utc(timestamp_str):
    """Validate ISO 8601 UTC format with Z suffix (CAP-040c)."""
    import re
    # ISO 8601 pattern: YYYY-MM-DDTHH:MM:SSZ
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    return bool(re.match(pattern, str(timestamp_str)))


def validate_schema(chatlog, config, debug_mode=False):
    """
    Validate chatlog schema structure (CAP-040a through CAP-040g).
    Returns (errors, warnings) tuple.
    """
    errors = []
    warnings = []

    # CAP-040a: Confirm all required top-level fields present
    required_fields = [
        'chatlog_id', 'schema_version', 'timestamp', 'agent',
        'session_duration_minutes', 'rules', 'session_context', 'artifacts'
    ]
    for field in required_fields:
        if field not in chatlog:
            errors.append({
                'category': 'top_level',
                'field': field,
                'error_type': 'missing_field',
                'message': f"Missing required field: {field}"
            })

    if errors:
        return errors, warnings  # Cannot continue validation without required fields

    # CAP-040b: Validate chatlog_id is valid UUID v4
    if not validate_uuid(chatlog.get('chatlog_id', '')):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_uuid',
            'message': f"chatlog_id '{chatlog.get('chatlog_id')}' is not a valid UUID v4"
        })

    # CAP-040c: Validate timestamp is ISO 8601 UTC with Z suffix
    if not validate_iso8601_utc(chatlog.get('timestamp', '')):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_timestamp',
            'message': f"timestamp '{chatlog.get('timestamp')}' is not ISO 8601 UTC format (missing Z suffix or invalid format)"
        })

    # CAP-040h: Debug mode - confirm schema_version matches deployed version
    if debug_mode:
        deployed_version = config['behavior']['chatlog_schema_version']
        chatlog_version = chatlog.get('schema_version', '')
        if chatlog_version != deployed_version:
            errors.append({
                'category': 'top_level',
                'field': 'schema_version',
                'error_type': 'version_mismatch',
                'message': f"Schema version mismatch: chatlog has {chatlog_version}, deployed expects {deployed_version}"
            })

    # Validate rules structure
    rules = chatlog.get('rules', {})
    if not isinstance(rules, dict):
        errors.append({
            'category': 'rules',
            'error_type': 'invalid_structure',
            'message': "rules field must be a dictionary"
        })
        return errors, warnings

    # CAP-040d, CAP-040e, CAP-040f, CAP-040g: Validate each rule category
    valid_domains = set(config['domain_tags'])
    confidence_threshold = 0.5  # CAP-040j: Extract.py filter threshold
    low_confidence_rules = []

    for category in ['decisions', 'constraints', 'invariants']:
        if category not in rules:
            continue

        rule_list = rules[category]
        if not isinstance(rule_list, list):
            errors.append({
                'category': category,
                'error_type': 'invalid_structure',
                'message': f"rules.{category} must be a list"
            })
            continue

        for idx, rule in enumerate(rule_list):
            if not isinstance(rule, dict):
                errors.append({
                    'category': category,
                    'index': idx,
                    'error_type': 'invalid_structure',
                    'message': f"Rule at {category} index {idx}: must be a dictionary"
                })
                continue

            # CAP-023: Each rule has: topic, rationale, domain, confidence
            required_rule_fields = ['topic', 'rationale', 'domain', 'confidence']
            for field in required_rule_fields:
                if field not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': field,
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing required field '{field}'"
                    })

            # CAP-040d: Validate domain is in allowed list
            domain = rule.get('domain', '')
            if domain not in valid_domains:
                errors.append({
                    'category': category,
                    'index': idx,
                    'field': 'domain',
                    'error_type': 'invalid_domain',
                    'message': f"Rule at {category} index {idx}: domain '{domain}' not in allowed list"
                })

            # CAP-040i: Debug mode - warn about domains not in current vocabulary
            if debug_mode and domain and domain not in valid_domains:
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': category,
                    'index': idx,
                    'field': 'domain',
                    'message': f"[MEDIUM] Domain '{domain}' not in current vocabulary. May be deprecated or renamed."
                })

            # CAP-040e: Validate confidence in [0.0, 1.0]
            confidence = rule.get('confidence')
            if confidence is not None:
                try:
                    conf_float = float(confidence)
                    if conf_float < 0.0 or conf_float > 1.0:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'confidence',
                            'error_type': 'out_of_range',
                            'message': f"Rule at {category} index {idx}: confidence {conf_float} out of range [0.0, 1.0]"
                        })
                    # CAP-040j: Track rules below threshold
                    if conf_float < confidence_threshold:
                        low_confidence_rules.append((category, idx, conf_float))
                except (ValueError, TypeError):
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'confidence',
                        'error_type': 'invalid_type',
                        'message': f"Rule at {category} index {idx}: confidence must be a number"
                    })

            # CAP-040f: Decisions have decision-specific fields
            if category == 'decisions':
                decision_fields = ['alternatives_rejected', 'context_when_applies', 'context_when_not', 'tradeoffs']
                for field in decision_fields:
                    if field not in rule:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': field,
                            'error_type': 'missing_field',
                            'message': f"Rule at {category} index {idx}: decisions require field '{field}'"
                        })

            # CAP-040g: Constraints have constraint-specific fields
            if category == 'constraints':
                if 'validation_method' not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'validation_method',
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: constraints require 'validation_method'"
                    })

    # CAP-040j: Report rules below confidence threshold
    if low_confidence_rules:
        total_rules = sum(len(rules.get(cat, [])) for cat in ['decisions', 'constraints', 'invariants'])
        count = len(low_confidence_rules)
        percentage = (count / total_rules * 100) if total_rules > 0 else 0
        warnings.append({
            'severity': 'INFO',
            'category': 'confidence',
            'message': f"[INFO] {count} rules ({percentage:.0f}%) below confidence threshold {confidence_threshold} - will be filtered by extract.py"
        })

    return errors, warnings


def validate_quality(chatlog):
    """
    Quality validation with non-blocking warnings (CAP-061 through CAP-065).
    Returns list of warnings with severity levels.
    """
    warnings = []
    rules = chatlog.get('rules', {})

    # CAP-061: Detect multi-behavior patterns in constraints
    multi_behavior_patterns = [
        ('and also', 'HIGH'),
        ('in addition', 'HIGH'),
        ('; ', 'MEDIUM'),
        (' and ', 'LOW')
    ]

    for constraint in rules.get('constraints', []):
        topic = constraint.get('topic', '').lower()
        for pattern, severity in multi_behavior_patterns:
            if pattern in topic:
                warnings.append({
                    'severity': severity,
                    'category': 'constraints',
                    'type': 'multi_behavior',
                    'pattern': pattern,
                    'message': f"[{severity}] Possible multi-behavior CON (contains '{pattern}'): {constraint.get('topic', '')[:80]}"
                })
                break  # Only report highest severity match

    # CAP-062: Detect temporal language patterns
    temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']

    for category in ['decisions', 'constraints', 'invariants']:
        for rule in rules.get(category, []):
            topic = rule.get('topic', '')
            rationale = rule.get('rationale', '')
            combined = (topic + ' ' + rationale).lower()

            for pattern in temporal_patterns:
                if pattern.lower() in combined:
                    warnings.append({
                        'severity': 'MEDIUM',
                        'category': category,
                        'type': 'temporal_language',
                        'pattern': pattern,
                        'message': f"[MEDIUM] Temporal language detected (contains '{pattern}'): {topic[:80]}"
                    })
                    break

    # CAP-063: Detect cross-domain boundary violations
    for category in ['decisions', 'constraints', 'invariants']:
        for rule in rules.get(category, []):
            topic = rule.get('topic', '').lower()
            rationale = rule.get('rationale', '').lower()
            combined = topic + ' ' + rationale

            # System Domain (model/) should not reference Build Domain (build/ or context engine)
            if 'model/' in combined and ('build/' in combined or 'context engine' in combined):
                warnings.append({
                    'severity': 'HIGH',
                    'category': category,
                    'type': 'cross_domain_boundary',
                    'message': f"[HIGH] Cross-domain boundary violation (model/ references build/): {topic[:80]}"
                })

    return warnings


def format_error_output(errors, warnings):
    """Format errors and warnings for JSON output (CAP-042, CAP-043a)."""
    result = {
        'valid': len(errors) == 0,
        'errors': [],
        'warnings': []
    }

    # CAP-043a: Parseable error format with category, index, field, error_type
    for error in errors:
        result['errors'].append({
            'category': error.get('category', 'unknown'),
            'index': error.get('index'),
            'field': error.get('field'),
            'error_type': error.get('error_type', 'unknown'),
            'message': error['message']
        })

    # CAP-065: Warnings include severity levels
    for warning in warnings:
        result['warnings'].append({
            'severity': warning.get('severity', 'INFO'),
            'category': warning.get('category', 'unknown'),
            'type': warning.get('type', 'quality'),
            'message': warning['message']
        })

    return result


def main():
    """
    Chatlog validation script with schema and quality checks.
    Implements CAP-040 through CAP-073.
    """
    # CAP-041a: Accept optional --debug flag
    debug_mode = '--debug' in sys.argv

    # Get chatlog file path
    if len(sys.argv) < 2:
        print(json.dumps({
            'valid': False,
            'errors': [{'message': 'Usage: validate_chatlog.py [--debug] <chatlog.yaml>'}]
        }))
        sys.exit(2)

    # Find chatlog path argument (skip --debug if present)
    chatlog_path = sys.argv[2] if debug_mode and len(sys.argv) > 2 else sys.argv[1]
    chatlog_file = Path(chatlog_path)

    # Check file exists
    if not chatlog_file.exists():
        print(json.dumps({
            'valid': False,
            'errors': [{'message': f'File not found: {chatlog_path}'}]
        }))
        sys.exit(2)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(json.dumps({
            'valid': False,
            'errors': [{'message': f'Error loading configuration: {e}'}]
        }))
        sys.exit(2)

    # CAP-020: Load chatlog YAML
    try:
        with open(chatlog_file) as f:
            chatlog = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(json.dumps({
            'valid': False,
            'errors': [{'message': f'YAML parse error: {e}'}]
        }))
        sys.exit(2)

    # CAP-040, CAP-064: Schema validation first, then quality validation
    schema_errors, schema_warnings = validate_schema(chatlog, config, debug_mode)

    # CAP-064: Quality warnings only if schema is valid
    quality_warnings = []
    if not schema_errors:
        quality_warnings = validate_quality(chatlog)

    # Combine warnings
    all_warnings = schema_warnings + quality_warnings

    # CAP-042: Format output as JSON
    result = format_error_output(schema_errors, all_warnings)

    # CAP-041a: Exit codes
    print(json.dumps(result, indent=2))

    if schema_errors:
        sys.exit(1)  # Invalid (schema errors or debug-mode version mismatch)
    else:
        sys.exit(0)  # Valid (no errors, may have warnings)


if __name__ == '__main__':
    sys.exit(main())
