#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-001 through CAP-090
Generated from: specs/modules/runtime-command-chatlog-capture-v1.14.1.yaml
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

    # Load tag vocabulary file (CAP-066, CAP-072)
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # Use domain strings as-is from vocabulary (no normalization per v1.14.1)
    config['domain_tags'] = list(vocabulary['tier_1_domains'].keys())

    return config


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_uuid(uuid_str):
    """Validate UUID v4 format (CAP-040b)."""
    import re
    # UUID v4 format: 8-4-4-4-12 hex characters
    pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    return bool(re.match(pattern, str(uuid_str).lower()))


def validate_timestamp(ts_str):
    """Validate ISO 8601 UTC with Z suffix (CAP-040c)."""
    import re
    # ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    return bool(re.match(pattern, str(ts_str)))


def validate_schema(chatlog, config, debug_mode=False):
    """
    Validate chatlog structure and return errors/warnings.

    Implements: CAP-040 through CAP-043, CAP-040h/i/j (debug mode),
                CAP-061 through CAP-065 (quality warnings)

    Returns: (errors, warnings) tuple of lists
    """
    errors = []
    warnings = []

    # CAP-040a: Required top-level fields
    required_fields = ['chatlog_id', 'schema_version', 'timestamp', 'agent',
                      'session_duration_minutes', 'rules', 'session_context', 'artifacts']
    for field in required_fields:
        if field not in chatlog:
            errors.append({
                'category': 'top_level',
                'field': field,
                'error_type': 'missing_field',
                'message': f"Missing required field: {field}"
            })

    if errors:
        return (errors, warnings)  # Early exit if top-level fields missing

    # CAP-040b: Validate UUID v4
    if not validate_uuid(chatlog['chatlog_id']):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_format',
            'message': f"Invalid UUID v4 format: {chatlog['chatlog_id']}"
        })

    # CAP-040c: Validate timestamp
    if not validate_timestamp(chatlog['timestamp']):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_format',
            'message': f"Invalid ISO 8601 UTC timestamp (must end with Z): {chatlog['timestamp']}"
        })

    # CAP-040h: Debug mode - schema version compatibility check
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

    # CAP-022: Validate rule categories
    rules = chatlog.get('rules', {})
    valid_categories = ['decisions', 'constraints', 'invariants']

    # Collect all rules for domain/confidence validation
    all_rules = []
    for category in valid_categories:
        category_rules = rules.get(category, [])
        if not isinstance(category_rules, list):
            errors.append({
                'category': 'rules',
                'field': f'rules.{category}',
                'error_type': 'invalid_type',
                'message': f"rules.{category} must be a list"
            })
            continue

        for idx, rule in enumerate(category_rules):
            all_rules.append((category, idx, rule))

            # CAP-023: Required rule fields
            required_rule_fields = ['topic', 'rationale', 'domain', 'confidence']
            for field in required_rule_fields:
                if field not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': field,
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing {field}"
                    })

            # CAP-040d: Validate domain
            if 'domain' in rule:
                if rule['domain'] not in config['domain_tags']:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'domain',
                        'error_type': 'invalid_value',
                        'message': f"Rule at {category} index {idx}: domain '{rule['domain']}' not in allowed list",
                        'value': rule['domain']
                    })

            # CAP-040e: Validate confidence range
            if 'confidence' in rule:
                try:
                    conf = float(rule['confidence'])
                    if not (0.0 <= conf <= 1.0):
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'confidence',
                            'error_type': 'out_of_range',
                            'message': f"Rule at {category} index {idx}: confidence {conf} not in [0.0, 1.0]",
                            'value': conf
                        })
                except (ValueError, TypeError):
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'confidence',
                        'error_type': 'invalid_type',
                        'message': f"Rule at {category} index {idx}: confidence must be a number"
                    })

            # CAP-040f: Decision-specific fields
            if category == 'decisions':
                decision_fields = ['alternatives_rejected', 'context_when_applies',
                                 'context_when_not', 'tradeoffs']
                for field in decision_fields:
                    if field not in rule:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': field,
                            'error_type': 'missing_field',
                            'message': f"Rule at {category} index {idx}: missing {field}"
                        })

            # CAP-040g: Constraint-specific fields
            if category == 'constraints':
                if 'validation_method' not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'validation_method',
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing validation_method"
                    })

    # CAP-026: Validate session_context structure
    session_ctx = chatlog.get('session_context', {})
    required_ctx_fields = ['problem_solved', 'patterns_applied', 'anti_patterns_avoided',
                           'conventions_established', 'reusability_scope']
    for field in required_ctx_fields:
        if field not in session_ctx:
            errors.append({
                'category': 'session_context',
                'field': field,
                'error_type': 'missing_field',
                'message': f"session_context missing required field: {field}"
            })

    # CAP-026b: Validate reusability_scope structure
    if 'reusability_scope' in session_ctx:
        scope = session_ctx['reusability_scope']
        if not isinstance(scope, dict):
            errors.append({
                'category': 'session_context',
                'field': 'reusability_scope',
                'error_type': 'invalid_type',
                'message': "reusability_scope must be a dictionary"
            })
        else:
            # CAP-026c/d/e: Required subfields
            for subfield in ['project_wide', 'module_scoped', 'historical']:
                if subfield not in scope:
                    errors.append({
                        'category': 'session_context',
                        'field': f'reusability_scope.{subfield}',
                        'error_type': 'missing_field',
                        'message': f"reusability_scope missing required field: {subfield}"
                    })

    # CAP-027: Validate artifacts structure
    artifacts = chatlog.get('artifacts', {})
    for field in ['files_modified', 'commands_executed']:
        if field not in artifacts:
            errors.append({
                'category': 'artifacts',
                'field': field,
                'error_type': 'missing_field',
                'message': f"artifacts missing required field: {field}"
            })

    # ========================================================================
    # QUALITY WARNINGS (CAP-061 through CAP-065)
    # Only run if schema validation passed
    # ========================================================================

    if not errors:
        # CAP-040i: Domain vocabulary warnings (debug mode only)
        if debug_mode:
            vocabulary_domains = set(config['domain_tags'])
            for category, idx, rule in all_rules:
                if 'domain' in rule and rule['domain'] not in vocabulary_domains:
                    warnings.append({
                        'severity': 'MEDIUM',
                        'category': category,
                        'index': idx,
                        'message': f"[MEDIUM] Domain '{rule['domain']}' not in current vocabulary tier-1 domains"
                    })

        # CAP-040j: Confidence threshold warnings (debug mode only)
        if debug_mode:
            threshold = 0.5  # Per EXT-021
            low_confidence_rules = [r for c, i, r in all_rules if r.get('confidence', 1.0) < threshold]
            if low_confidence_rules:
                total_rules = len(all_rules)
                count = len(low_confidence_rules)
                percentage = int((count / total_rules) * 100) if total_rules > 0 else 0
                warnings.append({
                    'severity': 'INFO',
                    'message': f"[INFO] {count} rules ({percentage}%) below confidence threshold {threshold} - will be filtered by extract.py"
                })

        # CAP-061: Multi-behavior pattern detection
        multi_behavior_patterns = [
            ('and also', 'HIGH'),
            ('in addition', 'HIGH'),
            ('; ', 'MEDIUM'),
            (' and ', 'LOW')
        ]

        for category, idx, rule in all_rules:
            if category == 'constraints':
                topic = rule.get('topic', '').lower()
                rationale = rule.get('rationale', '').lower()
                combined = f"{topic} {rationale}"

                for pattern, severity in multi_behavior_patterns:
                    if pattern in combined:
                        warnings.append({
                            'severity': severity,
                            'category': category,
                            'index': idx,
                            'message': f"[{severity}] Possible multi-behavior CON (contains '{pattern}'). Consider splitting per INV-002."
                        })
                        break  # Only report highest severity match

        # CAP-062: Temporal language detection
        temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']

        for category, idx, rule in all_rules:
            topic = rule.get('topic', '')
            rationale = rule.get('rationale', '')
            combined = f"{topic} {rationale}"

            for pattern in temporal_patterns:
                if pattern in combined:
                    warnings.append({
                        'severity': 'MEDIUM',
                        'category': category,
                        'index': idx,
                        'message': f"[MEDIUM] Temporal language detected (contains '{pattern}'). May be lifecycle candidate."
                    })
                    break  # Only report first match

        # CAP-063: Cross-domain boundary violations
        for category, idx, rule in all_rules:
            topic = rule.get('topic', '').lower()
            rationale = rule.get('rationale', '').lower()
            combined = f"{topic} {rationale}"

            if 'model/' in combined and ('build/' in combined or 'context engine' in combined):
                warnings.append({
                    'severity': 'HIGH',
                    'category': category,
                    'index': idx,
                    'message': "[HIGH] Cross-domain boundary violation: System Domain (model/) references Build Domain (build/ or context engine) per CON-00056"
                })

    return (errors, warnings)


# ============================================================================
# REMEDIATION PATTERNS (CAP-089)
# ============================================================================

def remediate_fuzzy_match_domain(rule, config):
    """Fuzzy match invalid domain to closest valid domain."""
    from difflib import get_close_matches

    invalid_domain = rule.get('domain', '')
    valid_domains = config['domain_tags']

    matches = get_close_matches(invalid_domain, valid_domains, n=1, cutoff=0.6)
    if matches:
        old_value = rule['domain']
        rule['domain'] = matches[0]
        return {'pattern': 'FUZZY_MATCH_DOMAIN', 'old': old_value, 'new': matches[0]}

    return None


def remediate_clamp_confidence(rule):
    """Clamp confidence to [0.0, 1.0] range."""
    conf = rule.get('confidence', 0.5)
    old_value = conf

    if conf > 1.0:
        rule['confidence'] = 0.95
    elif conf < 0.0:
        rule['confidence'] = 0.5
    else:
        return None

    return {'pattern': 'CLAMP_CONFIDENCE', 'old': old_value, 'new': rule['confidence']}


def remediate_regenerate_uuid(chatlog):
    """Generate new UUID v4."""
    import uuid
    old_value = chatlog.get('chatlog_id', '')
    chatlog['chatlog_id'] = str(uuid.uuid4())
    return {'pattern': 'REGENERATE_UUID', 'old': old_value, 'new': chatlog['chatlog_id']}


def remediate_regenerate_timestamp(chatlog):
    """Generate new ISO 8601 UTC timestamp."""
    from datetime import datetime
    old_value = chatlog.get('timestamp', '')
    chatlog['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    return {'pattern': 'REGENERATE_TIMESTAMP', 'old': old_value, 'new': chatlog['timestamp']}


def remediate_add_validation_method(rule):
    """Add default validation_method."""
    if 'validation_method' not in rule:
        rule['validation_method'] = 'Code review required'
        return {'pattern': 'ADD_VALIDATION_METHOD', 'old': None, 'new': 'Code review required'}
    return None


def remediate_add_reusability_scope_fields(session_context):
    """Add missing reusability_scope subfields."""
    fixes = []

    if 'reusability_scope' not in session_context:
        session_context['reusability_scope'] = {}

    scope = session_context['reusability_scope']

    if 'project_wide' not in scope:
        scope['project_wide'] = []
        fixes.append({'pattern': 'ADD_REUSABILITY_SCOPE_FIELDS', 'field': 'project_wide', 'old': None, 'new': []})

    if 'module_scoped' not in scope:
        scope['module_scoped'] = {}
        fixes.append({'pattern': 'ADD_REUSABILITY_SCOPE_FIELDS', 'field': 'module_scoped', 'old': None, 'new': {}})

    if 'historical' not in scope:
        scope['historical'] = []
        fixes.append({'pattern': 'ADD_REUSABILITY_SCOPE_FIELDS', 'field': 'historical', 'old': None, 'new': []})

    return fixes if fixes else None


def apply_remediation(chatlog, errors, config):
    """
    Apply automatic fixes based on error patterns (CAP-089).

    Returns: List of fixes applied
    """
    fixes_applied = []

    for error in errors:
        error_type = error.get('error_type')
        field = error.get('field', '')
        category = error.get('category')
        index = error.get('index')

        # FUZZY_MATCH_DOMAIN
        if error_type == 'invalid_value' and field == 'domain':
            if category and index is not None:
                rules = chatlog.get('rules', {}).get(category, [])
                if index < len(rules):
                    fix = remediate_fuzzy_match_domain(rules[index], config)
                    if fix:
                        fix['field'] = f'rules.{category}[{index}].domain'
                        fixes_applied.append(fix)

        # CLAMP_CONFIDENCE
        elif error_type == 'out_of_range' and field == 'confidence':
            if category and index is not None:
                rules = chatlog.get('rules', {}).get(category, [])
                if index < len(rules):
                    fix = remediate_clamp_confidence(rules[index])
                    if fix:
                        fix['field'] = f'rules.{category}[{index}].confidence'
                        fixes_applied.append(fix)

        # REGENERATE_UUID
        elif error_type == 'invalid_format' and field == 'chatlog_id':
            fix = remediate_regenerate_uuid(chatlog)
            if fix:
                fix['field'] = 'chatlog_id'
                fixes_applied.append(fix)

        # REGENERATE_TIMESTAMP
        elif error_type == 'invalid_format' and field == 'timestamp':
            fix = remediate_regenerate_timestamp(chatlog)
            if fix:
                fix['field'] = 'timestamp'
                fixes_applied.append(fix)

        # ADD_VALIDATION_METHOD
        elif error_type == 'missing_field' and field == 'validation_method':
            if category and index is not None:
                rules = chatlog.get('rules', {}).get(category, [])
                if index < len(rules):
                    fix = remediate_add_validation_method(rules[index])
                    if fix:
                        fix['field'] = f'rules.{category}[{index}].validation_method'
                        fixes_applied.append(fix)

        # ADD_REUSABILITY_SCOPE_FIELDS
        elif error_type == 'missing_field' and 'reusability_scope' in field:
            session_ctx = chatlog.get('session_context', {})
            fix_list = remediate_add_reusability_scope_fields(session_ctx)
            if fix_list:
                for fix in fix_list:
                    fix['field'] = f"session_context.reusability_scope.{fix['field']}"
                    fixes_applied.append(fix)

    return fixes_applied


# ============================================================================
# MAIN VALIDATION LOGIC
# ============================================================================

def validate_chatlog_file(filepath, config, debug_mode=False, remediate=False, max_attempts=3):
    """
    Validate chatlog file with optional remediation (CAP-087, CAP-088).

    Returns: dict with validation results
    """
    filepath = Path(filepath)

    # Check file exists
    if not filepath.exists():
        return {
            'success': False,
            'file': str(filepath.absolute()),
            'errors': [{'message': 'File not found'}],
            'warnings': [],
            'fixes_applied': []
        }

    # Load YAML
    try:
        with open(filepath) as f:
            chatlog = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return {
            'success': False,
            'file': str(filepath.absolute()),
            'errors': [{'message': f'YAML parse error: {e}'}],
            'warnings': [],
            'fixes_applied': []
        }

    if not isinstance(chatlog, dict):
        return {
            'success': False,
            'file': str(filepath.absolute()),
            'errors': [{'message': 'Chatlog must be a YAML dictionary'}],
            'warnings': [],
            'fixes_applied': []
        }

    all_fixes = []
    attempt = 0

    while attempt < max_attempts:
        attempt += 1

        # Validate
        errors, warnings = validate_schema(chatlog, config, debug_mode=debug_mode)

        if not errors:
            # Success - save if modified
            if remediate and all_fixes:
                with open(filepath, 'w') as f:
                    yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False)

            return {
                'success': True,
                'file': str(filepath.absolute()),
                'attempts': attempt,
                'errors': [],
                'warnings': warnings,
                'fixes_applied': all_fixes
            }

        # If remediation disabled or last attempt, return errors
        if not remediate or attempt >= max_attempts:
            return {
                'success': False,
                'file': str(filepath.absolute()),
                'attempts': attempt,
                'errors': errors,
                'warnings': warnings,
                'fixes_applied': all_fixes
            }

        # Apply remediation
        fixes = apply_remediation(chatlog, errors, config)
        all_fixes.extend(fixes)

        if not fixes:
            # No fixes applied, can't make progress
            return {
                'success': False,
                'file': str(filepath.absolute()),
                'attempts': attempt,
                'errors': errors,
                'warnings': warnings,
                'fixes_applied': all_fixes
            }

    # Max attempts exhausted
    return {
        'success': False,
        'file': str(filepath.absolute()),
        'attempts': attempt,
        'errors': errors,
        'warnings': warnings,
        'fixes_applied': all_fixes
    }


def main():
    """
    Chatlog validation with optional remediation.

    Usage:
        validate_chatlog.py <chatlog_file> [--debug] [--remediate] [--max-attempts N]

    Exit codes:
        0: Valid (no errors, may have warnings)
        1: Invalid (schema errors or debug-mode version mismatch)
        2: File not found or YAML parse error
    """
    import argparse

    parser = argparse.ArgumentParser(description='Validate chatlog YAML files')
    parser.add_argument('chatlog_file', help='Path to chatlog YAML file')
    parser.add_argument('--debug', action='store_true',
                       help='Enable deployment compatibility checks (CAP-041a)')
    parser.add_argument('--remediate', action='store_true',
                       help='Automatically fix common errors (CAP-087)')
    parser.add_argument('--max-attempts', type=int, default=3,
                       help='Maximum remediation attempts (default: 3)')

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        result = {
            'success': False,
            'errors': [{'message': f'Configuration error: {e}'}],
            'warnings': [],
            'fixes_applied': []
        }
        print(json.dumps(result, indent=2))
        return 2

    # Validate chatlog
    result = validate_chatlog_file(
        args.chatlog_file,
        config,
        debug_mode=args.debug,
        remediate=args.remediate,
        max_attempts=args.max_attempts
    )

    # Print JSON result (CAP-042, CAP-088)
    print(json.dumps(result, indent=2))

    # Determine exit code (CAP-041a)
    if result['success']:
        return 0
    elif 'File not found' in str(result.get('errors', [])) or 'YAML parse error' in str(result.get('errors', [])):
        return 2
    else:
        return 1


if __name__ == '__main__':
    sys.exit(main())
