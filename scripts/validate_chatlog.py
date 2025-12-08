#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-001 through CAP-090
Generated from: specs/modules/runtime-command-chatlog-capture-v1.14.1.yaml
"""

import sys
import json
import argparse
import re
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple
from difflib import get_close_matches

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

    Implements CAP-066: Loads vocabulary file from deployment config for domain validation.
    """
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    # CAP-066: Load tag vocabulary
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # CAP-066, v1.14.1: Use domain strings as-is from vocabulary (no normalization)
    # Domains are strings, not Python identifiers. UUIDs with hyphens are valid domains.
    config['domain_tags'] = list(vocabulary['tier_1_domains'].keys())

    return config


def validate_uuid(value: str) -> bool:
    """Validate UUID v4 format (CAP-040b)."""
    try:
        parsed = uuid.UUID(value, version=4)
        return str(parsed) == value
    except (ValueError, AttributeError):
        return False


def validate_timestamp(value: str) -> bool:
    """Validate ISO 8601 UTC timestamp with Z suffix (CAP-040c)."""
    # Handle datetime objects from YAML parser
    if isinstance(value, datetime):
        # YAML parsed it as datetime - convert back to string and validate
        value = value.strftime('%Y-%m-%dT%H:%M:%SZ')

    if not isinstance(value, str):
        return False

    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    if not re.match(pattern, value):
        return False
    try:
        datetime.fromisoformat(value.replace('Z', '+00:00'))
        return True
    except ValueError:
        return False


def validate_schema_structure(chatlog: Dict[str, Any], config: Dict[str, Any], debug: bool = False) -> Tuple[List[Dict], List[str]]:
    """Validate chatlog schema structure.

    Returns (errors, warnings) where errors block validation and warnings are informational.

    Implements:
    - CAP-040a through CAP-040g: Schema validation
    - CAP-040h: Schema version compatibility (debug mode only)
    - CAP-040i: Domain vocabulary warnings (debug mode only)
    - CAP-040j: Confidence threshold warnings (debug mode only)
    """
    errors = []
    warnings = []

    # CAP-040a: Validate required top-level fields
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
        return errors, warnings

    # CAP-040b: Validate chatlog_id is UUID v4
    if not validate_uuid(chatlog['chatlog_id']):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_format',
            'message': f"Invalid UUID v4 format: {chatlog['chatlog_id']}"
        })

    # CAP-040c: Validate timestamp is ISO 8601 UTC with Z suffix
    if not validate_timestamp(chatlog['timestamp']):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_format',
            'message': f"Invalid ISO 8601 UTC timestamp (expected YYYY-MM-DDTHH:MM:SSZ): {chatlog['timestamp']}"
        })

    # CAP-040h: Debug mode - validate schema version compatibility
    if debug:
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
    if 'rules' not in chatlog or not isinstance(chatlog['rules'], dict):
        errors.append({
            'category': 'rules',
            'field': 'rules',
            'error_type': 'invalid_structure',
            'message': "Rules must be a dictionary with decisions/constraints/invariants"
        })
        return errors, warnings

    rules = chatlog['rules']
    valid_domains = set(config['domain_tags'])
    all_rules = []

    # CAP-022: Validate rule categories
    for category in ['decisions', 'constraints', 'invariants']:
        if category not in rules:
            continue

        if not isinstance(rules[category], list):
            errors.append({
                'category': 'rules',
                'field': category,
                'error_type': 'invalid_type',
                'message': f"Rules category '{category}' must be a list"
            })
            continue

        for idx, rule in enumerate(rules[category]):
            all_rules.append(rule)

            # CAP-023: Validate required fields
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
            if 'domain' in rule and rule['domain'] not in valid_domains:
                errors.append({
                    'category': category,
                    'index': idx,
                    'field': 'domain',
                    'error_type': 'invalid_value',
                    'message': f"Rule at {category} index {idx}: domain '{rule['domain']}' not in allowed list {sorted(valid_domains)}"
                })

            # CAP-040e: Validate confidence is in [0.0, 1.0]
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

            # CAP-040f: Validate decision-specific fields
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

            # CAP-040g: Validate constraint-specific fields
            if category == 'constraints':
                if 'validation_method' not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'validation_method',
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing constraint field 'validation_method'"
                    })

    # CAP-040i: Debug mode - warn about domains not in current vocabulary
    if debug:
        vocabulary_domains = set(config['domain_tags'])
        for rule in all_rules:
            if 'domain' in rule and rule['domain'] not in vocabulary_domains:
                warnings.append(f"[MEDIUM] Domain '{rule['domain']}' not in current vocabulary tier-1 domains")

    # CAP-040j: Debug mode - report rules below confidence threshold
    if debug:
        threshold = 0.5  # Per EXT-021
        low_confidence_rules = [r for r in all_rules if 'confidence' in r and float(r['confidence']) < threshold]
        if low_confidence_rules:
            count = len(low_confidence_rules)
            total = len(all_rules)
            percentage = int(100 * count / total) if total > 0 else 0
            warnings.append(f"[INFO] {count} rules ({percentage}%) below confidence threshold {threshold} - will be filtered by extract.py")

    # CAP-026: Validate session_context structure
    if 'session_context' in chatlog:
        ctx = chatlog['session_context']
        if not isinstance(ctx, dict):
            errors.append({
                'category': 'session_context',
                'field': 'session_context',
                'error_type': 'invalid_type',
                'message': "session_context must be a dictionary"
            })
        else:
            # CAP-026b: Validate reusability_scope structure
            if 'reusability_scope' in ctx:
                scope = ctx['reusability_scope']
                if not isinstance(scope, dict):
                    errors.append({
                        'category': 'session_context',
                        'field': 'reusability_scope',
                        'error_type': 'invalid_type',
                        'message': "reusability_scope must be a dictionary"
                    })
                else:
                    # CAP-026c, CAP-026d, CAP-026e: Validate subfields
                    if 'project_wide' in scope and not isinstance(scope['project_wide'], list):
                        errors.append({
                            'category': 'session_context',
                            'field': 'reusability_scope.project_wide',
                            'error_type': 'invalid_type',
                            'message': "reusability_scope.project_wide must be a list"
                        })
                    if 'module_scoped' in scope and not isinstance(scope['module_scoped'], dict):
                        errors.append({
                            'category': 'session_context',
                            'field': 'reusability_scope.module_scoped',
                            'error_type': 'invalid_type',
                            'message': "reusability_scope.module_scoped must be a dictionary"
                        })
                    if 'historical' in scope and not isinstance(scope['historical'], list):
                        errors.append({
                            'category': 'session_context',
                            'field': 'reusability_scope.historical',
                            'error_type': 'invalid_type',
                            'message': "reusability_scope.historical must be a list"
                        })

    return errors, warnings


def validate_quality(chatlog: Dict[str, Any]) -> List[str]:
    """Run quality validators (non-blocking warnings).

    Implements:
    - CAP-061: Multi-behavior pattern detection
    - CAP-062: Temporal language detection
    - CAP-063: Cross-domain boundary violation detection
    - CAP-064: Non-blocking warnings
    - CAP-065: Severity levels
    """
    warnings = []

    if 'rules' not in chatlog:
        return warnings

    rules = chatlog['rules']

    for category in ['decisions', 'constraints', 'invariants']:
        if category not in rules:
            continue

        for idx, rule in enumerate(rules[category]):
            if 'topic' not in rule:
                continue

            topic = rule['topic']

            # CAP-061: Multi-behavior pattern detection
            multi_behavior_patterns = [
                ('and also', 'HIGH'),
                ('in addition', 'HIGH'),
                ('; ', 'MEDIUM'),
                (' and ', 'LOW')
            ]
            for pattern, severity in multi_behavior_patterns:
                if pattern in topic.lower():
                    warnings.append(f"[{severity}] Rule at {category} index {idx}: Possible multi-behavior (contains '{pattern}'). Consider splitting per INV-002.")
                    break  # Only warn once per rule

            # CAP-062: Temporal language detection
            temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']
            for pattern in temporal_patterns:
                if pattern in topic:
                    warnings.append(f"[MEDIUM] Rule at {category} index {idx}: Temporal language detected (contains '{pattern}'). May be lifecycle candidate.")
                    break  # Only warn once per rule

            # CAP-063: Cross-domain boundary violation detection
            if 'model/' in topic and ('build/' in topic or 'context engine' in topic.lower()):
                warnings.append(f"[HIGH] Rule at {category} index {idx}: Cross-domain boundary violation. System Domain (model/) references Build Domain. See CON-00056.")

    return warnings


def remediate_errors(chatlog: Dict[str, Any], errors: List[Dict], config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict]]:
    """Apply automatic remediation patterns to fix common errors.

    Implements CAP-089: Six remediation patterns.

    Returns (modified_chatlog, fixes_applied)
    """
    fixes_applied = []
    modified = chatlog.copy()

    for error in errors:
        error_type = error.get('error_type')
        field = error.get('field')
        category = error.get('category')
        index = error.get('index')

        # CAP-089: FUZZY_MATCH_DOMAIN
        if error_type == 'invalid_value' and field == 'domain':
            if category in ['decisions', 'constraints', 'invariants'] and index is not None:
                invalid_domain = modified['rules'][category][index]['domain']
                valid_domains = config['domain_tags']
                matches = get_close_matches(invalid_domain, valid_domains, n=1, cutoff=0.6)
                if matches:
                    old_value = invalid_domain
                    new_value = matches[0]
                    modified['rules'][category][index]['domain'] = new_value
                    fixes_applied.append({
                        'pattern': 'FUZZY_MATCH_DOMAIN',
                        'field': f'rules.{category}[{index}].domain',
                        'old': old_value,
                        'new': new_value
                    })

        # CAP-089: CLAMP_CONFIDENCE
        elif error_type == 'out_of_range' and field == 'confidence':
            if category in ['decisions', 'constraints', 'invariants'] and index is not None:
                old_value = modified['rules'][category][index]['confidence']
                if float(old_value) > 1.0:
                    new_value = 0.95
                elif float(old_value) < 0.0:
                    new_value = 0.5
                else:
                    new_value = old_value
                modified['rules'][category][index]['confidence'] = new_value
                fixes_applied.append({
                    'pattern': 'CLAMP_CONFIDENCE',
                    'field': f'rules.{category}[{index}].confidence',
                    'old': old_value,
                    'new': new_value
                })

        # CAP-089: REGENERATE_UUID
        elif error_type == 'invalid_format' and field == 'chatlog_id':
            old_value = modified.get('chatlog_id', '')
            new_value = str(uuid.uuid4())
            modified['chatlog_id'] = new_value
            fixes_applied.append({
                'pattern': 'REGENERATE_UUID',
                'field': 'chatlog_id',
                'old': old_value,
                'new': new_value
            })

        # CAP-089: REGENERATE_TIMESTAMP
        elif error_type == 'invalid_format' and field == 'timestamp':
            old_value = modified.get('timestamp', '')
            # Use timezone-aware datetime (Python 3.11+) or fallback to utcnow
            try:
                from datetime import UTC
                new_value = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
            except ImportError:
                new_value = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            modified['timestamp'] = new_value
            fixes_applied.append({
                'pattern': 'REGENERATE_TIMESTAMP',
                'field': 'timestamp',
                'old': old_value,
                'new': new_value
            })

        # CAP-089: ADD_VALIDATION_METHOD
        elif error_type == 'missing_field' and field == 'validation_method':
            if category == 'constraints' and index is not None:
                modified['rules'][category][index]['validation_method'] = 'Code review required'
                fixes_applied.append({
                    'pattern': 'ADD_VALIDATION_METHOD',
                    'field': f'rules.{category}[{index}].validation_method',
                    'old': None,
                    'new': 'Code review required'
                })

        # CAP-089: ADD_REUSABILITY_SCOPE_FIELDS
        elif error_type == 'missing_field' and 'reusability_scope' in field:
            if 'session_context' not in modified:
                modified['session_context'] = {}
            if 'reusability_scope' not in modified['session_context']:
                modified['session_context']['reusability_scope'] = {}

            scope = modified['session_context']['reusability_scope']
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

    return modified, fixes_applied


def validate_chatlog(chatlog_path: Path, config: Dict[str, Any], debug: bool = False,
                     remediate: bool = False, max_attempts: int = 3) -> Dict[str, Any]:
    """Main validation function.

    Implements:
    - CAP-040 through CAP-043: Validation logic
    - CAP-087, CAP-088: Remediation mode
    """
    # Load chatlog
    try:
        with open(chatlog_path) as f:
            chatlog = yaml.safe_load(f)
    except FileNotFoundError:
        return {
            'valid': False,
            'file': str(chatlog_path),
            'errors': ['File not found'],
            'warnings': []
        }
    except yaml.YAMLError as e:
        return {
            'valid': False,
            'file': str(chatlog_path),
            'errors': [f'YAML parse error: {e}'],
            'warnings': []
        }

    # CAP-087: Remediation loop
    if remediate:
        all_fixes = []
        for attempt in range(max_attempts):
            # Validate schema
            schema_errors, schema_warnings = validate_schema_structure(chatlog, config, debug)

            if not schema_errors:
                # Success - run quality checks
                quality_warnings = validate_quality(chatlog)
                return {
                    'success': True,
                    'valid': True,
                    'file': str(chatlog_path.absolute()),
                    'attempts': attempt + 1,
                    'fixes_applied': all_fixes,
                    'warnings': schema_warnings + quality_warnings,
                    'errors': []
                }

            # Apply remediation
            chatlog, fixes = remediate_errors(chatlog, schema_errors, config)
            all_fixes.extend(fixes)

            # Save modified chatlog
            with open(chatlog_path, 'w') as f:
                yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False)

        # Failed after max attempts
        schema_errors, schema_warnings = validate_schema_structure(chatlog, config, debug)
        return {
            'success': False,
            'valid': False,
            'file': str(chatlog_path.absolute()),
            'attempts': max_attempts,
            'fixes_applied': all_fixes,
            'warnings': schema_warnings,
            'errors': [e['message'] for e in schema_errors]
        }

    # Non-remediate mode: single validation pass
    schema_errors, schema_warnings = validate_schema_structure(chatlog, config, debug)

    if schema_errors:
        return {
            'valid': False,
            'file': str(chatlog_path),
            'errors': [e['message'] for e in schema_errors],
            'warnings': schema_warnings
        }

    # Schema valid - run quality checks
    quality_warnings = validate_quality(chatlog)

    return {
        'valid': True,
        'file': str(chatlog_path),
        'errors': [],
        'warnings': schema_warnings + quality_warnings
    }


def main():
    """Session knowledge extraction with Python-based validation and remediation (Spec 11 v2.0.0)"""
    parser = argparse.ArgumentParser(
        description='Validate chatlog YAML files with schema and quality checks'
    )
    parser.add_argument('chatlog_file', type=Path, help='Path to chatlog YAML file')
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
            'valid': False,
            'errors': [f'Configuration error: {e}'],
            'warnings': []
        }
        print(json.dumps(result, indent=2))
        return 2

    # Validate chatlog
    result = validate_chatlog(args.chatlog_file, config, args.debug, args.remediate, args.max_attempts)

    # CAP-042: Output JSON result
    print(json.dumps(result, indent=2))

    # CAP-041a: Exit codes
    if result['valid']:
        return 0  # Valid (possibly with warnings)
    else:
        if 'File not found' in result.get('errors', []) or any('YAML parse error' in e for e in result.get('errors', [])):
            return 2  # File not found or parse error
        else:
            return 1  # Invalid (schema errors)


if __name__ == '__main__':
    sys.exit(main())
