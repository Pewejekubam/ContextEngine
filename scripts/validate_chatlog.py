#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-001 through CAP-090
Generated from: specs/modules/runtime-command-chatlog-capture-v1.14.0.yaml
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
# Gracefully handle missing config during module import (for --help, etc)
try:
    with open(CONFIG_PATH) as f:
        _config = yaml.safe_load(f)
        PROJECT_ROOT = Path(_config['paths']['project_root'])
        # Read context_engine_home from config - allows .context-engine to be placed anywhere
        BASE_DIR = Path(_config['paths']['context_engine_home'])
except FileNotFoundError:
    # Config will be loaded in main() with proper error handling
    PROJECT_ROOT = None
    pass


def load_config():
    """Load deployment configuration and vocabulary (CAP-066)."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    # Load tag vocabulary (CAP-066, CAP-072)
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    try:
        with open(vocab_path) as f:
            vocabulary = yaml.safe_load(f)

        # Extract domains with hyphen->underscore normalization for Python compatibility (CAP-067)
        config['domain_tags'] = [
            d.replace('-', '_') for d in vocabulary.get('tier_1_domains', [])
        ]
    except FileNotFoundError:
        # Graceful fallback if vocabulary doesn't exist yet
        config['domain_tags'] = []

    return config


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

import re
import uuid
import argparse
import difflib
from datetime import datetime


def validate_uuid(value):
    """Validate UUID v4 format (CAP-040b)."""
    try:
        uuid_obj = uuid.UUID(str(value), version=4)
        return str(uuid_obj) == str(value)
    except (ValueError, AttributeError):
        return False


def validate_iso8601_utc(value):
    """Validate ISO 8601 UTC timestamp with Z suffix (CAP-040c)."""
    # YAML parser auto-converts ISO 8601 strings to datetime objects
    # Accept both string format and datetime objects
    if isinstance(value, datetime):
        # If it's already a datetime, check if it's UTC
        return value.tzinfo is not None

    if not isinstance(value, str) or not value.endswith('Z'):
        return False

    # Pattern: YYYY-MM-DDTHH:MM:SSZ
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    if not re.match(pattern, value):
        return False

    try:
        # Verify it's a valid datetime
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return True
    except ValueError:
        return False


def validate_schema(chatlog, config, debug_mode=False):
    """
    Validate chatlog schema structure (CAP-040a through CAP-040j).

    Returns: (errors, warnings) tuple
    """
    errors = []
    warnings = []

    # CAP-040a: Required top-level fields
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
        return errors, warnings  # Can't continue if missing top-level fields

    # CAP-040b: Validate chatlog_id is UUID v4
    if not validate_uuid(chatlog['chatlog_id']):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_format',
            'message': f"chatlog_id must be valid UUID v4, got: {chatlog['chatlog_id']}"
        })

    # CAP-040c: Validate timestamp is ISO 8601 UTC with Z suffix
    if not validate_iso8601_utc(chatlog['timestamp']):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_format',
            'message': f"timestamp must be ISO 8601 UTC with Z suffix, got: {chatlog['timestamp']}"
        })

    # CAP-040h: Debug mode - validate schema version matches deployment (BLOCKING)
    if debug_mode:
        deployed_version = config.get('behavior', {}).get('chatlog_schema_version', 'v1.13.0')
        chatlog_version = chatlog.get('schema_version', '')
        if chatlog_version != deployed_version:
            errors.append({
                'category': 'top_level',
                'field': 'schema_version',
                'error_type': 'version_mismatch',
                'message': f"Schema version mismatch: chatlog has {chatlog_version}, deployed expects {deployed_version}"
            })

    # Validate rules structure
    if not isinstance(chatlog['rules'], dict):
        errors.append({
            'category': 'rules',
            'field': 'rules',
            'error_type': 'invalid_type',
            'message': "rules must be a dictionary with category keys"
        })
        return errors, warnings

    # CAP-040d, CAP-040e, CAP-040f, CAP-040g: Validate each rule category
    domain_tags = set(config.get('domain_tags', []))
    all_rules = []

    for category in ['decisions', 'constraints', 'invariants']:
        if category not in chatlog['rules']:
            continue

        rules = chatlog['rules'][category]
        if not isinstance(rules, list):
            errors.append({
                'category': category,
                'field': category,
                'error_type': 'invalid_type',
                'message': f"{category} must be a list of rules"
            })
            continue

        for idx, rule in enumerate(rules):
            all_rules.append(rule)

            # CAP-023: Required fields for all rules
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

            # CAP-040d: Validate domain is in vocabulary
            if 'domain' in rule:
                if domain_tags and rule['domain'] not in domain_tags:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'domain',
                        'error_type': 'invalid_value',
                        'message': f"Rule at {category} index {idx}: domain '{rule['domain']}' not in allowed list: {sorted(domain_tags)}"
                    })

            # CAP-040e: Validate confidence in [0.0, 1.0]
            if 'confidence' in rule:
                try:
                    conf = float(rule['confidence'])
                    if not (0.0 <= conf <= 1.0):
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'confidence',
                            'error_type': 'out_of_range',
                            'message': f"Rule at {category} index {idx}: confidence must be in [0.0, 1.0], got: {conf}"
                        })
                except (ValueError, TypeError):
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'confidence',
                        'error_type': 'invalid_type',
                        'message': f"Rule at {category} index {idx}: confidence must be a number, got: {rule['confidence']}"
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
                            'message': f"Rule at {category} index {idx}: decision missing field '{field}'"
                        })

            # CAP-040g: Constraint-specific fields
            if category == 'constraints':
                if 'validation_method' not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'validation_method',
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: constraint missing field 'validation_method'"
                    })

    # CAP-026: Validate session_context structure
    if 'session_context' in chatlog:
        ctx = chatlog['session_context']
        if isinstance(ctx, dict):
            # CAP-026b: Validate reusability_scope structure
            if 'reusability_scope' in ctx:
                scope = ctx['reusability_scope']
                if isinstance(scope, dict):
                    # Check for required subfields
                    if 'project_wide' not in scope:
                        errors.append({
                            'category': 'session_context',
                            'field': 'reusability_scope.project_wide',
                            'error_type': 'missing_field',
                            'message': "session_context.reusability_scope missing 'project_wide' field"
                        })
                    if 'module_scoped' not in scope:
                        errors.append({
                            'category': 'session_context',
                            'field': 'reusability_scope.module_scoped',
                            'error_type': 'missing_field',
                            'message': "session_context.reusability_scope missing 'module_scoped' field"
                        })
                    if 'historical' not in scope:
                        errors.append({
                            'category': 'session_context',
                            'field': 'reusability_scope.historical',
                            'error_type': 'missing_field',
                            'message': "session_context.reusability_scope missing 'historical' field"
                        })

    # CAP-040i: Debug mode - warn about deprecated domains
    if debug_mode and domain_tags:
        for rule in all_rules:
            if 'domain' in rule and rule['domain'] not in domain_tags:
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': 'domain_vocabulary',
                    'message': f"Domain '{rule['domain']}' not in current vocabulary tier-1 domains"
                })

    # CAP-040j: Debug mode - report low confidence rules
    if debug_mode:
        threshold = 0.5
        low_confidence_rules = [r for r in all_rules if r.get('confidence', 1.0) < threshold]
        if low_confidence_rules:
            count = len(low_confidence_rules)
            total = len(all_rules)
            pct = (count / total * 100) if total > 0 else 0
            warnings.append({
                'severity': 'INFO',
                'category': 'confidence_threshold',
                'message': f"{count} rules ({pct:.0f}%) below confidence threshold {threshold} - will be filtered by extract.py"
            })

    return errors, warnings


def validate_quality(chatlog):
    """
    Quality validation with non-blocking warnings (CAP-061 through CAP-065).

    Returns: list of warning objects with severity levels
    """
    warnings = []

    # Get all rules
    all_rules = []
    for category in ['decisions', 'constraints', 'invariants']:
        if category in chatlog.get('rules', {}):
            for idx, rule in enumerate(chatlog['rules'][category]):
                all_rules.append((category, idx, rule))

    # CAP-061: Multi-behavior pattern detection
    multi_behavior_patterns = [
        ('and also', 'HIGH'),
        ('in addition', 'HIGH'),
        ('; ', 'MEDIUM'),
        (' and ', 'LOW')
    ]

    for category, idx, rule in all_rules:
        topic = rule.get('topic', '')

        # Only check constraints for multi-behavior (CAP-061)
        if category == 'constraints':
            for pattern, severity in multi_behavior_patterns:
                if pattern in topic.lower():
                    warnings.append({
                        'severity': severity,
                        'category': category,
                        'index': idx,
                        'rule_type': 'multi_behavior',
                        'message': f"[{severity}] Rule at {category} index {idx}: Possible multi-behavior CON (contains '{pattern}'). Consider splitting per INV-002."
                    })
                    break  # Only report highest severity match

        # CAP-062: Temporal language detection
        temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']
        for pattern in temporal_patterns:
            if pattern in topic or pattern in rule.get('rationale', ''):
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': category,
                    'index': idx,
                    'rule_type': 'temporal_language',
                    'message': f"[MEDIUM] Rule at {category} index {idx}: Temporal language detected (contains '{pattern}'). May be lifecycle candidate."
                })
                break

        # CAP-063: Cross-domain boundary violations
        text = f"{topic} {rule.get('rationale', '')}"
        if 'model/' in text and ('build/' in text or 'context engine' in text.lower()):
            warnings.append({
                'severity': 'HIGH',
                'category': category,
                'index': idx,
                'rule_type': 'boundary_violation',
                'message': f"[HIGH] Rule at {category} index {idx}: Cross-domain boundary violation (System Domain references Build Domain). See CON-00056."
            })

    return warnings


# ============================================================================
# REMEDIATION FUNCTIONS (CAP-089)
# ============================================================================

def remediate_fuzzy_match_domain(chatlog, error, config):
    """Pattern: FUZZY_MATCH_DOMAIN - use difflib to find nearest valid domain."""
    category = error['category']
    idx = error['index']

    domain_tags = config.get('domain_tags', [])
    if not domain_tags:
        return False

    invalid_domain = chatlog['rules'][category][idx]['domain']

    # Find closest match
    matches = difflib.get_close_matches(invalid_domain, domain_tags, n=1, cutoff=0.6)
    if matches:
        old_value = invalid_domain
        new_value = matches[0]
        chatlog['rules'][category][idx]['domain'] = new_value
        return {
            'pattern': 'FUZZY_MATCH_DOMAIN',
            'field': f'rules.{category}[{idx}].domain',
            'old': old_value,
            'new': new_value
        }

    return False


def remediate_clamp_confidence(chatlog, error):
    """Pattern: CLAMP_CONFIDENCE - clamp to [0.0, 1.0] range."""
    category = error['category']
    idx = error['index']

    old_value = chatlog['rules'][category][idx]['confidence']

    # Clamp: >1.0 → 0.95, <0.0 → 0.5
    if old_value > 1.0:
        new_value = 0.95
    elif old_value < 0.0:
        new_value = 0.5
    else:
        new_value = max(0.0, min(1.0, old_value))

    chatlog['rules'][category][idx]['confidence'] = new_value

    return {
        'pattern': 'CLAMP_CONFIDENCE',
        'field': f'rules.{category}[{idx}].confidence',
        'old': old_value,
        'new': new_value
    }


def remediate_regenerate_uuid(chatlog, error):
    """Pattern: REGENERATE_UUID - generate new UUID v4."""
    old_value = chatlog['chatlog_id']
    new_value = str(uuid.uuid4())
    chatlog['chatlog_id'] = new_value

    return {
        'pattern': 'REGENERATE_UUID',
        'field': 'chatlog_id',
        'old': old_value,
        'new': new_value
    }


def remediate_regenerate_timestamp(chatlog, error):
    """Pattern: REGENERATE_TIMESTAMP - generate new ISO 8601 UTC timestamp."""
    old_value = chatlog['timestamp']
    new_value = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    chatlog['timestamp'] = new_value

    return {
        'pattern': 'REGENERATE_TIMESTAMP',
        'field': 'timestamp',
        'old': old_value,
        'new': new_value
    }


def remediate_add_validation_method(chatlog, error):
    """Pattern: ADD_VALIDATION_METHOD - add default validation method."""
    category = error['category']
    idx = error['index']

    chatlog['rules'][category][idx]['validation_method'] = 'Code review required'

    return {
        'pattern': 'ADD_VALIDATION_METHOD',
        'field': f'rules.{category}[{idx}].validation_method',
        'old': None,
        'new': 'Code review required'
    }


def remediate_add_reusability_scope_fields(chatlog, error):
    """Pattern: ADD_REUSABILITY_SCOPE_FIELDS - add missing subfields."""
    if 'session_context' not in chatlog:
        chatlog['session_context'] = {}

    if 'reusability_scope' not in chatlog['session_context']:
        chatlog['session_context']['reusability_scope'] = {}

    scope = chatlog['session_context']['reusability_scope']
    fixes = []

    if 'project_wide' not in scope:
        scope['project_wide'] = []
        fixes.append('project_wide')

    if 'module_scoped' not in scope:
        scope['module_scoped'] = {}
        fixes.append('module_scoped')

    if 'historical' not in scope:
        scope['historical'] = []
        fixes.append('historical')

    if fixes:
        return {
            'pattern': 'ADD_REUSABILITY_SCOPE_FIELDS',
            'field': 'session_context.reusability_scope',
            'old': None,
            'new': f"Added: {', '.join(fixes)}"
        }

    return False


def remediate_chatlog(chatlog, errors, config):
    """
    Apply remediation patterns to fix errors (CAP-089).

    Returns: list of applied fixes
    """
    fixes = []

    for error in errors:
        fix = None

        # Pattern matching based on error type and field
        if error['error_type'] == 'invalid_value' and error['field'] == 'domain':
            fix = remediate_fuzzy_match_domain(chatlog, error, config)

        elif error['error_type'] == 'out_of_range' and error['field'] == 'confidence':
            fix = remediate_clamp_confidence(chatlog, error)

        elif error['error_type'] == 'invalid_format' and error['field'] == 'chatlog_id':
            fix = remediate_regenerate_uuid(chatlog, error)

        elif error['error_type'] == 'invalid_format' and error['field'] == 'timestamp':
            fix = remediate_regenerate_timestamp(chatlog, error)

        elif error['error_type'] == 'missing_field' and error['field'] == 'validation_method':
            fix = remediate_add_validation_method(chatlog, error)

        elif error['error_type'] == 'missing_field' and 'reusability_scope' in error['field']:
            fix = remediate_add_reusability_scope_fields(chatlog, error)

        if fix:
            fixes.append(fix)

    return fixes


def validate_chatlog_file(chatlog_path, config, debug_mode=False, remediate_mode=False, max_attempts=3):
    """
    Validate chatlog file with optional remediation (CAP-087).

    Returns: result dict with success, errors, warnings, fixes_applied
    """
    chatlog_path = Path(chatlog_path)

    # Load chatlog
    try:
        with open(chatlog_path) as f:
            chatlog = yaml.safe_load(f)
    except FileNotFoundError:
        return {
            'success': False,
            'file': str(chatlog_path.absolute()),
            'errors': [{'message': f"File not found: {chatlog_path}"}],
            'warnings': [],
            'fixes_applied': []
        }
    except yaml.YAMLError as e:
        return {
            'success': False,
            'file': str(chatlog_path.absolute()),
            'errors': [{'message': f"YAML parse error: {e}"}],
            'warnings': [],
            'fixes_applied': []
        }

    all_fixes = []
    attempts = 0

    while attempts < max_attempts:
        attempts += 1

        # Run schema validation
        errors, warnings = validate_schema(chatlog, config, debug_mode)

        if not errors:
            # No errors - run quality validation
            quality_warnings = validate_quality(chatlog)
            warnings.extend(quality_warnings)

            # Success
            return {
                'success': True,
                'file': str(chatlog_path.absolute()),
                'attempts': attempts,
                'errors': [],
                'warnings': warnings,
                'fixes_applied': all_fixes
            }

        # If remediate mode and errors found, try to fix
        if remediate_mode and attempts < max_attempts:
            fixes = remediate_chatlog(chatlog, errors, config)
            all_fixes.extend(fixes)

            if not fixes:
                # No fixes applied, can't remediate further
                break

            # Save remediated version
            with open(chatlog_path, 'w') as f:
                yaml.safe_dump(chatlog, f, default_flow_style=False, allow_unicode=True)
        else:
            break

    # Failed validation
    return {
        'success': False,
        'file': str(chatlog_path.absolute()),
        'attempts': attempts,
        'errors': errors,
        'warnings': warnings,
        'fixes_applied': all_fixes
    }


def main():
    """Chatlog validation with optional remediation (CAP-041, CAP-087)."""
    parser = argparse.ArgumentParser(
        description='Validate chatlog YAML files with schema and quality checks'
    )
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
            'errors': [{'message': f"Error loading configuration: {e}"}],
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
        remediate_mode=args.remediate,
        max_attempts=args.max_attempts
    )

    # Output JSON result (CAP-042, CAP-088)
    print(json.dumps(result, indent=2))

    # Exit codes (CAP-041a)
    if result['success']:
        return 0  # Valid
    elif 'YAML parse error' in str(result.get('errors', [])) or 'File not found' in str(result.get('errors', [])):
        return 2  # File/parse error
    else:
        return 1  # Invalid after max attempts


if __name__ == '__main__':
    sys.exit(main())
