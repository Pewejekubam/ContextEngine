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


def load_config():
    """Load deployment configuration and vocabulary (CAP-066)."""
    global BASE_DIR  # Update BASE_DIR based on config

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
        # Read context_engine_home from config - allows .context-engine to be placed anywhere
        BASE_DIR = Path(config['paths']['context_engine_home'])

    # CAP-066: Load tag vocabulary from deployment config
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # CAP-066: Extract domains with hyphen->underscore normalization for Python compatibility
    config['domain_tags'] = [
        d.replace('-', '_') for d in vocabulary.get('tier_1_domains', [])
    ]

    return config


# ============================================================================
# VALIDATION FUNCTIONS (CAP-040 through CAP-065)
# ============================================================================

import re
import uuid
from datetime import datetime
from difflib import get_close_matches


def validate_uuid(value):
    """Validate UUID v4 format (CAP-040b)."""
    try:
        uuid_obj = uuid.UUID(str(value), version=4)
        return str(uuid_obj) == str(value)
    except (ValueError, AttributeError):
        return False


def validate_iso8601_utc(value):
    """Validate ISO 8601 UTC timestamp with Z suffix (CAP-040c)."""
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    if not re.match(pattern, str(value)):
        return False
    try:
        datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return True
    except ValueError:
        return False


def validate_chatlog_structure(chatlog, config, debug_mode=False):
    """
    Validate chatlog structure and return errors/warnings (CAP-040 through CAP-065).

    Args:
        chatlog: Parsed YAML chatlog dictionary
        config: Deployment configuration with domain_tags
        debug_mode: Enable deployment compatibility checks (CAP-041a)

    Returns:
        tuple: (errors, warnings) where each is a list of error/warning dicts
    """
    errors = []
    warnings = []

    # CAP-040a: Validation confirms all required top-level fields present
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

    # CAP-040b: Validation confirms chatlog_id is valid UUID v4
    if 'chatlog_id' in chatlog and not validate_uuid(chatlog['chatlog_id']):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_format',
            'message': f"chatlog_id '{chatlog['chatlog_id']}' is not a valid UUID v4"
        })

    # CAP-040c: Validation confirms timestamp is ISO 8601 UTC with Z suffix
    if 'timestamp' in chatlog and not validate_iso8601_utc(chatlog['timestamp']):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_format',
            'message': f"timestamp '{chatlog.get('timestamp')}' is not ISO 8601 UTC with Z suffix"
        })

    # CAP-040h: Debug mode - schema version compatibility check
    if debug_mode and 'schema_version' in chatlog:
        deployed_version = config.get('behavior', {}).get('chatlog_schema_version')
        chatlog_version = chatlog['schema_version']
        if deployed_version and chatlog_version != deployed_version:
            errors.append({
                'category': 'deployment_compatibility',
                'field': 'schema_version',
                'error_type': 'version_mismatch',
                'message': f"Schema version mismatch: chatlog has {chatlog_version}, deployed expects {deployed_version}"
            })

    # Validate rules structure
    if 'rules' not in chatlog:
        return (errors, warnings)

    rules = chatlog['rules']
    domain_tags = config.get('domain_tags', [])
    all_rules = []

    # Validate each rule category
    for category in ['decisions', 'constraints', 'invariants']:
        if category not in rules:
            continue

        for idx, rule in enumerate(rules[category]):
            all_rules.append(rule)

            # CAP-023: Each rule has: topic, rationale, domain, confidence
            for field in ['topic', 'rationale', 'domain', 'confidence']:
                if field not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': field,
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing required field '{field}'"
                    })

            # CAP-040d: Validation confirms all rule domains are in domain_tags
            if 'domain' in rule and rule['domain'] not in domain_tags:
                errors.append({
                    'category': category,
                    'index': idx,
                    'field': 'domain',
                    'error_type': 'invalid_value',
                    'message': f"Rule at {category} index {idx}: domain '{rule['domain']}' not in allowed list {domain_tags}"
                })

            # CAP-040e: Validation confirms all rule confidence values in [0.0, 1.0]
            if 'confidence' in rule:
                try:
                    conf = float(rule['confidence'])
                    if not (0.0 <= conf <= 1.0):
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'confidence',
                            'error_type': 'out_of_range',
                            'message': f"Rule at {category} index {idx}: confidence {conf} not in range [0.0, 1.0]"
                        })
                except (ValueError, TypeError):
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'confidence',
                        'error_type': 'invalid_type',
                        'message': f"Rule at {category} index {idx}: confidence must be a number"
                    })

            # CAP-040f: Decisions have all decision-specific fields
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
                            'message': f"Rule at {category} index {idx}: missing decision field '{field}'"
                        })

            # CAP-040g: Constraints have all constraint-specific fields
            if category == 'constraints':
                if 'validation_method' not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'validation_method',
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing constraint field 'validation_method'"
                    })

            # Quality warnings (CAP-061 through CAP-063) - non-blocking
            if 'topic' in rule or 'rationale' in rule:
                text = f"{rule.get('topic', '')} {rule.get('rationale', '')}"

                # CAP-061: Multi-behavior patterns in constraints
                if category == 'constraints':
                    if 'and also' in text:
                        warnings.append({
                            'category': category,
                            'index': idx,
                            'severity': 'HIGH',
                            'message': f"[HIGH] Rule at {category} index {idx}: Possible multi-behavior CON (contains 'and also'). Consider splitting per INV-002."
                        })
                    elif 'in addition' in text:
                        warnings.append({
                            'category': category,
                            'index': idx,
                            'severity': 'HIGH',
                            'message': f"[HIGH] Rule at {category} index {idx}: Possible multi-behavior CON (contains 'in addition'). Consider splitting per INV-002."
                        })
                    elif '; ' in text:
                        warnings.append({
                            'category': category,
                            'index': idx,
                            'severity': 'MEDIUM',
                            'message': f"[MEDIUM] Rule at {category} index {idx}: Possible multi-behavior CON (contains '; '). Review for split."
                        })
                    elif ' and ' in text:
                        warnings.append({
                            'category': category,
                            'index': idx,
                            'severity': 'LOW',
                            'message': f"[LOW] Rule at {category} index {idx}: Possible multi-behavior CON (contains ' and '). May be legitimate compound."
                        })

                # CAP-062: Temporal language patterns
                temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']
                for pattern in temporal_patterns:
                    if pattern in text:
                        warnings.append({
                            'category': category,
                            'index': idx,
                            'severity': 'MEDIUM',
                            'message': f"[MEDIUM] Rule at {category} index {idx}: Temporal language detected (contains '{pattern}'). May indicate lifecycle candidate."
                        })
                        break

                # CAP-063: Cross-domain boundary violations
                if 'model/' in text and ('build/' in text or 'context engine' in text.lower()):
                    warnings.append({
                        'category': category,
                        'index': idx,
                        'severity': 'MEDIUM',
                        'message': f"[MEDIUM] Rule at {category} index {idx}: Cross-domain boundary violation (System Domain references Build Domain). See CON-00056."
                    })

    # CAP-040i: Debug mode - domain vocabulary currency warning
    if debug_mode:
        vocabulary_domains = set(domain_tags)
        for rule in all_rules:
            if 'domain' in rule and rule['domain'] not in vocabulary_domains:
                warnings.append({
                    'severity': 'MEDIUM',
                    'message': f"[MEDIUM] Domain '{rule['domain']}' not in current vocabulary. May be deprecated or renamed."
                })

    # CAP-040j: Debug mode - low confidence rules report
    if debug_mode:
        threshold = 0.5
        low_confidence_rules = [r for r in all_rules if r.get('confidence', 1.0) < threshold]
        if low_confidence_rules:
            count = len(low_confidence_rules)
            total = len(all_rules)
            percentage = int((count / total) * 100) if total > 0 else 0
            warnings.append({
                'severity': 'INFO',
                'message': f"[INFO] {count} rules ({percentage}%) below confidence threshold {threshold} - will be filtered by extract.py"
            })

    # CAP-026: Session context validation
    if 'session_context' in chatlog:
        ctx = chatlog['session_context']
        ctx_fields = ['problem_solved', 'patterns_applied', 'anti_patterns_avoided',
                     'conventions_established', 'reusability_scope']
        for field in ctx_fields:
            if field not in ctx:
                errors.append({
                    'category': 'session_context',
                    'field': field,
                    'error_type': 'missing_field',
                    'message': f"session_context missing required field: {field}"
                })

        # CAP-026b: reusability_scope structure validation
        if 'reusability_scope' in ctx:
            scope = ctx['reusability_scope']
            if 'project_wide' not in scope:
                errors.append({
                    'category': 'session_context',
                    'field': 'reusability_scope.project_wide',
                    'error_type': 'missing_field',
                    'message': "reusability_scope missing 'project_wide' field"
                })
            if 'module_scoped' not in scope:
                errors.append({
                    'category': 'session_context',
                    'field': 'reusability_scope.module_scoped',
                    'error_type': 'missing_field',
                    'message': "reusability_scope missing 'module_scoped' field"
                })
            if 'historical' not in scope:
                errors.append({
                    'category': 'session_context',
                    'field': 'reusability_scope.historical',
                    'error_type': 'missing_field',
                    'message': "reusability_scope missing 'historical' field"
                })

    # CAP-027: Artifacts validation
    if 'artifacts' in chatlog:
        artifacts = chatlog['artifacts']
        for field in ['files_modified', 'commands_executed']:
            if field not in artifacts:
                errors.append({
                    'category': 'artifacts',
                    'field': field,
                    'error_type': 'missing_field',
                    'message': f"artifacts missing required field: {field}"
                })

    return (errors, warnings)


# ============================================================================
# REMEDIATION FUNCTIONS (CAP-086 through CAP-089)
# ============================================================================

def remediate_chatlog(chatlog, errors, config):
    """
    Apply automatic remediation patterns to chatlog (CAP-089).

    Returns:
        tuple: (modified_chatlog, fixes_applied) where fixes_applied is a list of fix dicts
    """
    fixes_applied = []

    for error in errors:
        category = error.get('category')
        index = error.get('index')
        field = error.get('field')
        error_type = error.get('error_type')

        # CAP-089: FUZZY_MATCH_DOMAIN
        if error_type == 'invalid_value' and field == 'domain':
            invalid_domain = chatlog['rules'][category][index]['domain']
            valid_domains = config.get('domain_tags', [])
            matches = get_close_matches(invalid_domain, valid_domains, n=1, cutoff=0.6)
            new_domain = matches[0] if matches else valid_domains[0] if valid_domains else 'unknown'
            chatlog['rules'][category][index]['domain'] = new_domain
            fixes_applied.append({
                'pattern': 'FUZZY_MATCH_DOMAIN',
                'field': f'rules.{category}[{index}].domain',
                'old': invalid_domain,
                'new': new_domain
            })

        # CAP-089: CLAMP_CONFIDENCE
        elif error_type == 'out_of_range' and field == 'confidence':
            old_conf = chatlog['rules'][category][index]['confidence']
            if float(old_conf) > 1.0:
                new_conf = 0.95
            elif float(old_conf) < 0.0:
                new_conf = 0.5
            else:
                new_conf = old_conf
            chatlog['rules'][category][index]['confidence'] = new_conf
            fixes_applied.append({
                'pattern': 'CLAMP_CONFIDENCE',
                'field': f'rules.{category}[{index}].confidence',
                'old': old_conf,
                'new': new_conf
            })

        # CAP-089: REGENERATE_UUID
        elif error_type == 'invalid_format' and field == 'chatlog_id':
            old_uuid = chatlog.get('chatlog_id', '')
            new_uuid = str(uuid.uuid4())
            chatlog['chatlog_id'] = new_uuid
            fixes_applied.append({
                'pattern': 'REGENERATE_UUID',
                'field': 'chatlog_id',
                'old': old_uuid,
                'new': new_uuid
            })

        # CAP-089: REGENERATE_TIMESTAMP
        elif error_type == 'invalid_format' and field == 'timestamp':
            old_ts = chatlog.get('timestamp', '')
            new_ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            chatlog['timestamp'] = new_ts
            fixes_applied.append({
                'pattern': 'REGENERATE_TIMESTAMP',
                'field': 'timestamp',
                'old': old_ts,
                'new': new_ts
            })

        # CAP-089: ADD_VALIDATION_METHOD
        elif error_type == 'missing_field' and field == 'validation_method':
            chatlog['rules'][category][index]['validation_method'] = "Code review required"
            fixes_applied.append({
                'pattern': 'ADD_VALIDATION_METHOD',
                'field': f'rules.{category}[{index}].validation_method',
                'old': None,
                'new': "Code review required"
            })

        # CAP-089: ADD_REUSABILITY_SCOPE_FIELDS
        elif error_type == 'missing_field' and 'reusability_scope' in field:
            if 'session_context' not in chatlog:
                chatlog['session_context'] = {}
            if 'reusability_scope' not in chatlog['session_context']:
                chatlog['session_context']['reusability_scope'] = {}

            scope = chatlog['session_context']['reusability_scope']
            if 'project_wide' not in scope:
                scope['project_wide'] = []
            if 'module_scoped' not in scope:
                scope['module_scoped'] = {}
            if 'historical' not in scope:
                scope['historical'] = []

            fixes_applied.append({
                'pattern': 'ADD_REUSABILITY_SCOPE_FIELDS',
                'field': 'session_context.reusability_scope',
                'old': None,
                'new': scope
            })

    return (chatlog, fixes_applied)


def main():
    """
    Chatlog validation with optional remediation (CAP-087).

    Usage:
        validate_chatlog.py <chatlog_file> [--debug] [--remediate] [--max-attempts N]

    Exit codes:
        0: Valid (possibly after remediation)
        1: Invalid after max attempts
        2: File not found or YAML parse error
    """
    import argparse

    parser = argparse.ArgumentParser(description='Validate chatlog YAML structure')
    parser.add_argument('chatlog_file', help='Path to chatlog YAML file')
    parser.add_argument('--debug', action='store_true', help='Enable deployment compatibility checks (CAP-041a)')
    parser.add_argument('--remediate', action='store_true', help='Enable automatic remediation (CAP-087)')
    parser.add_argument('--max-attempts', type=int, default=3, help='Maximum remediation attempts (default: 3)')

    args = parser.parse_args()

    chatlog_path = Path(args.chatlog_file)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        result = {
            'valid': False,
            'errors': [{'message': f"Error loading configuration: {e}"}]
        }
        print(json.dumps(result, indent=2))
        return 2

    # Load chatlog file
    if not chatlog_path.exists():
        result = {
            'valid': False,
            'errors': [{'message': f"File not found: {chatlog_path}"}]
        }
        print(json.dumps(result, indent=2))
        return 2

    try:
        with open(chatlog_path) as f:
            chatlog = yaml.safe_load(f)
    except yaml.YAMLError as e:
        result = {
            'valid': False,
            'errors': [{'message': f"YAML parse error: {e}"}]
        }
        print(json.dumps(result, indent=2))
        return 2

    # CAP-087: Remediation mode with multiple attempts
    if args.remediate:
        all_fixes = []
        for attempt in range(1, args.max_attempts + 1):
            errors, warnings = validate_chatlog_structure(chatlog, config, args.debug)

            if not errors:
                # Success!
                result = {
                    'success': True,
                    'file': str(chatlog_path.absolute()),
                    'attempts': attempt,
                    'fixes_applied': all_fixes,
                    'warnings': [w.get('message', str(w)) for w in warnings],
                    'errors': []
                }
                print(json.dumps(result, indent=2))
                return 0

            # Apply remediation
            chatlog, fixes = remediate_chatlog(chatlog, errors, config)
            all_fixes.extend(fixes)

            # Write back to file
            with open(chatlog_path, 'w') as f:
                yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False)

        # Failed after max attempts
        errors, warnings = validate_chatlog_structure(chatlog, config, args.debug)
        result = {
            'success': False,
            'file': str(chatlog_path.absolute()),
            'attempts': args.max_attempts,
            'fixes_applied': all_fixes,
            'warnings': [w.get('message', str(w)) for w in warnings],
            'errors': [e.get('message', str(e)) for e in errors]
        }
        print(json.dumps(result, indent=2))
        return 1

    # CAP-042: Standard validation mode
    else:
        errors, warnings = validate_chatlog_structure(chatlog, config, args.debug)

        if errors:
            result = {
                'valid': False,
                'errors': [e.get('message', str(e)) for e in errors],
                'warnings': [w.get('message', str(w)) for w in warnings]
            }
            print(json.dumps(result, indent=2))
            return 1
        else:
            result = {
                'valid': True,
                'warnings': [w.get('message', str(w)) for w in warnings]
            }
            print(json.dumps(result, indent=2))
            return 0


if __name__ == '__main__':
    sys.exit(main())
