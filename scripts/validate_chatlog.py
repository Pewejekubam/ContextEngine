#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-001 through CAP-090
Generated from: build/modules/runtime-command-chatlog-capture.yaml v1.14.1
"""

import sys
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
import difflib
import argparse

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
    """Load deployment configuration and vocabulary.

    Implements CAP-066: Validator loads vocabulary file from deployment config.
    """
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    # Load tag vocabulary (CAP-066)
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # CAP-066: Use domain strings as-is from vocabulary (no normalization)
    # v1.14.1: CAP-067 removed - domains are not Python identifiers
    config['domain_tags'] = list(vocabulary['tier_1_domains'].keys())

    return config


def validate_uuid(value):
    """Validate UUID v4 format (CAP-040b)."""
    try:
        uuid_obj = uuid.UUID(value, version=4)
        return str(uuid_obj) == value
    except (ValueError, AttributeError):
        return False


def validate_timestamp(value):
    """Validate ISO 8601 UTC with Z suffix (CAP-040c)."""
    if not isinstance(value, str):
        return False
    if not value.endswith('Z'):
        return False
    try:
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return True
    except ValueError:
        return False


def validate_confidence(value):
    """Validate confidence is in [0.0, 1.0] range (CAP-040e)."""
    try:
        conf = float(value)
        return 0.0 <= conf <= 1.0
    except (ValueError, TypeError):
        return False


def validate_schema(chatlog, config, debug_mode=False):
    """Validate chatlog schema structure.

    Implements CAP-040 through CAP-040g (schema validation).
    Implements CAP-040h through CAP-040j (deployment compatibility in debug mode).

    Args:
        chatlog: Parsed YAML chatlog data
        config: Deployment configuration
        debug_mode: Enable deployment compatibility checks (CAP-041a)

    Returns:
        tuple: (errors_list, warnings_list)
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

    # If missing critical fields, return early
    if any(err['field'] in ['chatlog_id', 'schema_version', 'timestamp', 'rules'] for err in errors):
        return errors, warnings

    # CAP-040b: Validate chatlog_id is UUID v4
    if not validate_uuid(chatlog['chatlog_id']):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_format',
            'message': f"chatlog_id must be valid UUID v4, got: {chatlog['chatlog_id']}"
        })

    # CAP-040c: Validate timestamp is ISO 8601 UTC with Z suffix
    if not validate_timestamp(chatlog['timestamp']):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_format',
            'message': f"timestamp must be ISO 8601 UTC with Z suffix, got: {chatlog['timestamp']}"
        })

    # CAP-040h: Debug mode - validate schema_version matches deployment (BLOCKING)
    if debug_mode:
        deployed_version = config['behavior']['chatlog_schema_version']
        chatlog_version = chatlog['schema_version']
        if chatlog_version != deployed_version:
            errors.append({
                'category': 'top_level',
                'field': 'schema_version',
                'error_type': 'version_mismatch',
                'message': f"Schema version mismatch: chatlog has {chatlog_version}, deployed expects {deployed_version}"
            })

    # Collect all rules for validation
    all_rules = []
    rule_categories = ['decisions', 'constraints', 'invariants']

    for category in rule_categories:
        if category not in chatlog['rules']:
            continue

        category_rules = chatlog['rules'][category]
        if not isinstance(category_rules, list):
            errors.append({
                'category': category,
                'error_type': 'invalid_type',
                'message': f"rules.{category} must be a list"
            })
            continue

        for idx, rule in enumerate(category_rules):
            all_rules.append((category, idx, rule))

            # CAP-023: Each rule has required fields
            required_rule_fields = ['topic', 'rationale', 'domain', 'confidence']
            for field in required_rule_fields:
                if field not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': field,
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: Missing required field '{field}'"
                    })

            # CAP-040d: Validate domain is in allowed list
            if 'domain' in rule:
                if rule['domain'] not in config['domain_tags']:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'domain',
                        'error_type': 'invalid_value',
                        'message': f"Rule at {category} index {idx}: domain '{rule['domain']}' not in allowed list: {config['domain_tags']}"
                    })

            # CAP-040e: Validate confidence in [0.0, 1.0]
            if 'confidence' in rule:
                if not validate_confidence(rule['confidence']):
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'confidence',
                        'error_type': 'out_of_range',
                        'message': f"Rule at {category} index {idx}: confidence must be in [0.0, 1.0], got: {rule['confidence']}"
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
                            'message': f"Rule at {category} index {idx}: Missing decision field '{field}'"
                        })

            # CAP-040g: Constraints have constraint-specific fields
            if category == 'constraints':
                if 'validation_method' not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'validation_method',
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: Missing constraint field 'validation_method'"
                    })

    # CAP-040i: Debug mode - warn about domains not in current vocabulary
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

    # CAP-040j: Debug mode - report rules below confidence threshold
    if debug_mode:
        threshold = 0.5  # Per EXT-021
        low_confidence_rules = [r for _, _, r in all_rules if 'confidence' in r and r['confidence'] < threshold]
        if low_confidence_rules:
            total_rules = len(all_rules)
            count = len(low_confidence_rules)
            percentage = int((count / total_rules) * 100) if total_rules > 0 else 0
            warnings.append({
                'severity': 'INFO',
                'message': f"[INFO] {count} rules ({percentage}%) below confidence threshold {threshold} - will be filtered by extract.py"
            })

    # CAP-026: Validate session_context structure
    if 'session_context' in chatlog:
        ctx = chatlog['session_context']
        # CAP-026b: Validate reusability_scope structure
        if 'reusability_scope' in ctx:
            scope = ctx['reusability_scope']
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

    return errors, warnings


def validate_quality(chatlog):
    """Run quality validators (non-blocking warnings).

    Implements CAP-061 through CAP-065 (quality validation).
    Returns list of warnings with severity levels.
    """
    warnings = []

    # Collect all constraints
    constraints = []
    if 'rules' in chatlog and 'constraints' in chatlog['rules']:
        for idx, rule in enumerate(chatlog['rules']['constraints']):
            constraints.append((idx, rule))

    # CAP-061: Detect multi-behavior patterns in constraints
    for idx, rule in constraints:
        if 'topic' not in rule and 'rationale' not in rule:
            continue

        text = (rule.get('topic', '') + ' ' + rule.get('rationale', '')).lower()

        # High severity patterns
        if 'and also' in text:
            warnings.append({
                'severity': 'HIGH',
                'category': 'constraints',
                'index': idx,
                'message': f"[HIGH] Rule at constraints index {idx}: Possible multi-behavior CON (contains 'and also'). Consider splitting per INV-002."
            })
        elif 'in addition' in text:
            warnings.append({
                'severity': 'HIGH',
                'category': 'constraints',
                'index': idx,
                'message': f"[HIGH] Rule at constraints index {idx}: Possible multi-behavior CON (contains 'in addition'). Consider splitting per INV-002."
            })
        # Medium severity patterns
        elif '; ' in text:
            warnings.append({
                'severity': 'MEDIUM',
                'category': 'constraints',
                'index': idx,
                'message': f"[MEDIUM] Rule at constraints index {idx}: Possible multi-behavior CON (contains semicolon). Consider splitting per INV-002."
            })
        # Low severity patterns
        elif ' and ' in text and text.count(' and ') > 1:
            warnings.append({
                'severity': 'LOW',
                'category': 'constraints',
                'index': idx,
                'message': f"[LOW] Rule at constraints index {idx}: Multiple 'and' conjunctions detected. Verify single behavior per INV-002."
            })

    # CAP-062: Detect temporal language patterns
    temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']

    for category in ['decisions', 'constraints', 'invariants']:
        if 'rules' not in chatlog or category not in chatlog['rules']:
            continue

        for idx, rule in enumerate(chatlog['rules'][category]):
            if 'topic' not in rule and 'rationale' not in rule:
                continue

            text = rule.get('topic', '') + ' ' + rule.get('rationale', '')

            for pattern in temporal_patterns:
                if pattern in text:
                    warnings.append({
                        'severity': 'MEDIUM',
                        'category': category,
                        'index': idx,
                        'message': f"[MEDIUM] Rule at {category} index {idx}: Temporal language detected (contains '{pattern.strip()}'). May indicate lifecycle candidate."
                    })
                    break  # Only report once per rule

    # CAP-063: Detect cross-domain boundary violations
    for category in ['decisions', 'constraints', 'invariants']:
        if 'rules' not in chatlog or category not in chatlog['rules']:
            continue

        for idx, rule in enumerate(chatlog['rules'][category]):
            if 'topic' not in rule and 'rationale' not in rule:
                continue

            text = (rule.get('topic', '') + ' ' + rule.get('rationale', '')).lower()

            # System Domain (model/) should not reference Build Domain (build/ or context engine)
            if 'model/' in text and ('build/' in text or 'context engine' in text):
                warnings.append({
                    'severity': 'HIGH',
                    'category': category,
                    'index': idx,
                    'message': f"[HIGH] Rule at {category} index {idx}: Cross-domain boundary violation (references both model/ and build/context engine). See CON-00056."
                })

    return warnings


def remediate_errors(chatlog, errors, config):
    """Apply automatic remediation patterns to fix common errors.

    Implements CAP-089: Six remediation patterns as Python functions.

    Args:
        chatlog: Parsed YAML chatlog data (will be modified in place)
        errors: List of error dictionaries from validation
        config: Deployment configuration

    Returns:
        list: Applied fixes with pattern name and details
    """
    fixes_applied = []

    for error in errors:
        pattern_applied = None
        fix_details = {
            'field': error.get('field', 'unknown'),
            'category': error.get('category', 'unknown'),
            'index': error.get('index', -1)
        }

        # PATTERN 1: FUZZY_MATCH_DOMAIN
        if error['error_type'] == 'invalid_value' and error['field'] == 'domain':
            category = error['category']
            idx = error['index']
            rule = chatlog['rules'][category][idx]
            old_domain = rule['domain']

            # Use difflib to find closest match
            matches = difflib.get_close_matches(old_domain, config['domain_tags'], n=1, cutoff=0.6)
            if matches:
                new_domain = matches[0]
                rule['domain'] = new_domain
                pattern_applied = 'FUZZY_MATCH_DOMAIN'
                fix_details['old'] = old_domain
                fix_details['new'] = new_domain

        # PATTERN 2: CLAMP_CONFIDENCE
        elif error['error_type'] == 'out_of_range' and error['field'] == 'confidence':
            category = error['category']
            idx = error['index']
            rule = chatlog['rules'][category][idx]
            old_conf = rule['confidence']

            # Clamp to [0.0, 1.0]
            if old_conf > 1.0:
                new_conf = 0.95
            elif old_conf < 0.0:
                new_conf = 0.5
            else:
                new_conf = 0.5  # Invalid type

            rule['confidence'] = new_conf
            pattern_applied = 'CLAMP_CONFIDENCE'
            fix_details['old'] = old_conf
            fix_details['new'] = new_conf

        # PATTERN 3: REGENERATE_UUID
        elif error['error_type'] == 'invalid_format' and error['field'] == 'chatlog_id':
            new_uuid = str(uuid.uuid4())
            old_uuid = chatlog.get('chatlog_id', 'missing')
            chatlog['chatlog_id'] = new_uuid
            pattern_applied = 'REGENERATE_UUID'
            fix_details['old'] = old_uuid
            fix_details['new'] = new_uuid

        # PATTERN 4: REGENERATE_TIMESTAMP
        elif error['error_type'] == 'invalid_format' and error['field'] == 'timestamp':
            new_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            old_timestamp = chatlog.get('timestamp', 'missing')
            chatlog['timestamp'] = new_timestamp
            pattern_applied = 'REGENERATE_TIMESTAMP'
            fix_details['old'] = old_timestamp
            fix_details['new'] = new_timestamp

        # PATTERN 5: ADD_VALIDATION_METHOD
        elif error['error_type'] == 'missing_field' and error['field'] == 'validation_method':
            category = error['category']
            idx = error['index']
            rule = chatlog['rules'][category][idx]
            rule['validation_method'] = 'Code review required'
            pattern_applied = 'ADD_VALIDATION_METHOD'
            fix_details['new'] = 'Code review required'

        # PATTERN 6: ADD_REUSABILITY_SCOPE_FIELDS
        elif error['error_type'] == 'missing_field' and 'reusability_scope' in error['field']:
            if 'session_context' not in chatlog:
                chatlog['session_context'] = {}
            if 'reusability_scope' not in chatlog['session_context']:
                chatlog['session_context']['reusability_scope'] = {}

            scope = chatlog['session_context']['reusability_scope']

            if 'project_wide' in error['field']:
                scope['project_wide'] = []
                pattern_applied = 'ADD_REUSABILITY_SCOPE_FIELDS'
                fix_details['new'] = 'project_wide: []'
            elif 'module_scoped' in error['field']:
                scope['module_scoped'] = {}
                pattern_applied = 'ADD_REUSABILITY_SCOPE_FIELDS'
                fix_details['new'] = 'module_scoped: {}'
            elif 'historical' in error['field']:
                scope['historical'] = []
                pattern_applied = 'ADD_REUSABILITY_SCOPE_FIELDS'
                fix_details['new'] = 'historical: []'

        if pattern_applied:
            fix_details['pattern'] = pattern_applied
            fixes_applied.append(fix_details)

    return fixes_applied


def validate_chatlog_file(chatlog_path, config, debug_mode=False, remediate=False, max_attempts=3):
    """Validate a chatlog file with optional remediation.

    Implements CAP-087: validate_chatlog.py --remediate performs validation + auto-fix.
    Implements CAP-088: Remediate mode returns structured JSON documenting all actions.

    Args:
        chatlog_path: Path to chatlog YAML file
        config: Deployment configuration
        debug_mode: Enable deployment compatibility checks (CAP-041a)
        remediate: Enable automatic remediation (CAP-087)
        max_attempts: Maximum remediation attempts (CAP-045a, CAP-046)

    Returns:
        dict: Validation result with success status, errors, warnings, and fixes
    """
    chatlog_path = Path(chatlog_path)

    # Load chatlog YAML
    try:
        with open(chatlog_path) as f:
            chatlog = yaml.safe_load(f)
    except FileNotFoundError:
        return {
            'success': False,
            'file': str(chatlog_path),
            'errors': [{'message': f"File not found: {chatlog_path}"}],
            'exit_code': 2
        }
    except yaml.YAMLError as e:
        return {
            'success': False,
            'file': str(chatlog_path),
            'errors': [{'message': f"YAML parse error: {e}"}],
            'exit_code': 2
        }

    all_fixes = []
    attempt = 0

    # Remediation loop (CAP-045a, CAP-046)
    while attempt < max_attempts:
        attempt += 1

        # Run schema validation
        errors, schema_warnings = validate_schema(chatlog, config, debug_mode)

        # If valid, run quality validation (CAP-064)
        quality_warnings = []
        if not errors:
            quality_warnings = validate_quality(chatlog)

        all_warnings = schema_warnings + quality_warnings

        # If no errors, validation passed
        if not errors:
            return {
                'success': True,
                'file': str(chatlog_path.resolve()),
                'attempts': attempt,
                'fixes_applied': all_fixes,
                'warnings': all_warnings,
                'errors': [],
                'exit_code': 0
            }

        # If remediate mode disabled, return errors
        if not remediate:
            return {
                'success': False,
                'file': str(chatlog_path.resolve()),
                'attempts': attempt,
                'fixes_applied': [],
                'warnings': all_warnings,
                'errors': errors,
                'exit_code': 1
            }

        # Apply remediation patterns (CAP-089)
        fixes = remediate_errors(chatlog, errors, config)
        all_fixes.extend(fixes)

        # If no fixes applied, cannot remediate further
        if not fixes:
            break

        # Save modified chatlog for next iteration
        with open(chatlog_path, 'w') as f:
            yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Failed after max attempts
    # CAP-048: Save as .invalid with error comments
    invalid_path = chatlog_path.with_suffix('.invalid')

    # Create error header
    error_header = "# VALIDATION FAILED\n# Errors:\n"
    for error in errors:
        error_header += f"#   {error['message']}\n"
    error_header += "\n"

    # Write invalid file with error header
    with open(invalid_path, 'w') as f:
        f.write(error_header)
        yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    return {
        'success': False,
        'file': str(chatlog_path.resolve()),
        'invalid_file': str(invalid_path.resolve()),
        'attempts': attempt,
        'fixes_applied': all_fixes,
        'warnings': all_warnings,
        'errors': errors,
        'exit_code': 1
    }


def main():
    """Session knowledge extraction with Python-based validation and remediation (Spec 11 v2.0.0)"""
    parser = argparse.ArgumentParser(
        description='Validate chatlog YAML files with optional remediation'
    )
    parser.add_argument('chatlog_file', help='Path to chatlog YAML file')
    parser.add_argument('--debug', action='store_true',
                       help='Enable deployment compatibility checks (CAP-041a)')
    parser.add_argument('--remediate', action='store_true',
                       help='Enable automatic remediation (CAP-087)')
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
            'exit_code': 2
        }
        print(json.dumps(result, indent=2))
        return result['exit_code']

    # Validate chatlog file
    result = validate_chatlog_file(
        args.chatlog_file,
        config,
        debug_mode=args.debug,
        remediate=args.remediate,
        max_attempts=args.max_attempts
    )

    # Print result JSON (CAP-042, CAP-088)
    print(json.dumps(result, indent=2))

    # Print human-readable summary to stderr
    if result['success']:
        print(f"\n✓ Validation PASSED", file=sys.stderr)
        if result.get('fixes_applied'):
            print(f"  Applied {len(result['fixes_applied'])} automatic fixes", file=sys.stderr)
        if result.get('warnings'):
            print(f"  {len(result['warnings'])} warnings (non-blocking)", file=sys.stderr)
    else:
        print(f"\n✗ Validation FAILED", file=sys.stderr)
        print(f"  {len(result['errors'])} errors found", file=sys.stderr)
        if result.get('invalid_file'):
            print(f"  Saved as: {result['invalid_file']}", file=sys.stderr)

    return result['exit_code']


if __name__ == '__main__':
    sys.exit(main())
