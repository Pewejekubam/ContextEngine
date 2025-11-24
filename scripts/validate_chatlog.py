#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-040 through CAP-069d, RREL-002, RREL-009
Generated from: build/modules/runtime-command-chatlog-capture.yaml v1.13.0
"""

import sys
import json
import re
import uuid
from pathlib import Path
from datetime import datetime

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml

# INV-021: Absolute paths only - read from config
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
CONFIG_PATH = BASE_DIR / "config" / "deployment.yaml"


def load_config():
    """
    Load deployment configuration and vocabulary.

    CAP-066: Validator loads vocabulary file from deployment config for domain validation
    """
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    # Load tag vocabulary (CAP-066)
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # Extract tier 1 domains with hyphen->underscore normalization (CAP-066, CAP-067)
    # Python identifiers require underscores, not hyphens
    config['domain_tags'] = [
        d.replace('-', '_') for d in vocabulary.get('tier_1_domains', [])
    ]

    return config


def validate_uuid(value):
    """CAP-040b: Validate UUID v4 format"""
    try:
        parsed = uuid.UUID(value, version=4)
        return str(parsed) == value
    except (ValueError, AttributeError):
        return False


def validate_iso8601_utc(value):
    """CAP-040c: Validate ISO 8601 UTC timestamp with Z suffix"""
    if not isinstance(value, str):
        return False

    # Must end with Z
    if not value.endswith('Z'):
        return False

    # Try parsing ISO 8601 format
    try:
        datetime.fromisoformat(value.replace('Z', '+00:00'))
        return True
    except (ValueError, AttributeError):
        return False


def validate_schema(chatlog, config, debug_mode=False):
    """
    Perform schema validation (blocking errors).

    CAP-040a through CAP-040g: Required field and format validation
    CAP-040h through CAP-040j: Debug mode deployment compatibility checks
    """
    errors = []

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

    # CAP-040b: Validate chatlog_id is UUID v4
    if 'chatlog_id' in chatlog:
        if not validate_uuid(chatlog['chatlog_id']):
            errors.append({
                'category': 'top_level',
                'field': 'chatlog_id',
                'error_type': 'invalid_format',
                'message': f"chatlog_id '{chatlog['chatlog_id']}' is not a valid UUID v4"
            })

    # CAP-040c: Validate timestamp is ISO 8601 UTC with Z suffix
    if 'timestamp' in chatlog:
        if not validate_iso8601_utc(chatlog['timestamp']):
            errors.append({
                'category': 'top_level',
                'field': 'timestamp',
                'error_type': 'invalid_format',
                'message': f"timestamp '{chatlog['timestamp']}' is not ISO 8601 UTC format (must end with Z)"
            })

    # CAP-040h: Debug mode - validate schema version matches deployment
    if debug_mode and 'schema_version' in chatlog:
        deployed_version = config.get('behavior', {}).get('chatlog_schema_version')
        chatlog_version = chatlog['schema_version']
        if deployed_version and chatlog_version != deployed_version:
            errors.append({
                'category': 'top_level',
                'field': 'schema_version',
                'error_type': 'version_mismatch',
                'message': f"Schema version mismatch: chatlog has {chatlog_version}, deployed expects {deployed_version}"
            })

    # Validate rules section
    if 'rules' in chatlog:
        rules = chatlog['rules']

        # CAP-022: Rules categorized as decisions, constraints, invariants
        for category in ['decisions', 'constraints', 'invariants']:
            if category not in rules:
                continue

            for idx, rule in enumerate(rules[category]):
                rule_prefix = f"Rule at {category} index {idx}"

                # CAP-023: Required fields for all rules
                required_rule_fields = ['topic', 'rationale', 'domain', 'confidence']
                for field in required_rule_fields:
                    if field not in rule:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': field,
                            'error_type': 'missing_field',
                            'message': f"{rule_prefix}: Missing required field '{field}'"
                        })

                # CAP-040d: Validate domain is in allowed list
                if 'domain' in rule:
                    domain = rule['domain']
                    valid_domains = config.get('domain_tags', [])
                    if domain not in valid_domains:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'domain',
                            'error_type': 'invalid_value',
                            'message': f"{rule_prefix}: domain '{domain}' not in allowed list: {valid_domains}"
                        })

                # CAP-040e: Validate confidence in [0.0, 1.0]
                if 'confidence' in rule:
                    confidence = rule['confidence']
                    if not isinstance(confidence, (int, float)) or confidence < 0.0 or confidence > 1.0:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'confidence',
                            'error_type': 'out_of_range',
                            'message': f"{rule_prefix}: confidence {confidence} out of range [0.0, 1.0]"
                        })

                # CAP-040f: Decision-specific fields
                if category == 'decisions':
                    decision_fields = [
                        'alternatives_rejected', 'context_when_applies',
                        'context_when_not', 'tradeoffs'
                    ]
                    for field in decision_fields:
                        if field not in rule:
                            errors.append({
                                'category': category,
                                'index': idx,
                                'field': field,
                                'error_type': 'missing_field',
                                'message': f"{rule_prefix}: Missing decision field '{field}'"
                            })

                # CAP-040g: Constraint-specific fields
                if category == 'constraints':
                    if 'validation_method' not in rule:
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'validation_method',
                            'error_type': 'missing_field',
                            'message': f"{rule_prefix}: Missing constraint field 'validation_method'"
                        })

                # RREL-002: Validate relationships structure (optional field)
                if 'relationships' in rule:
                    if not isinstance(rule['relationships'], list):
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'relationships',
                            'error_type': 'invalid_type',
                            'message': f"{rule_prefix}: relationships must be a list"
                        })
                    else:
                        for rel_idx, rel in enumerate(rule['relationships']):
                            if not isinstance(rel, dict):
                                errors.append({
                                    'category': category,
                                    'index': idx,
                                    'field': f'relationships[{rel_idx}]',
                                    'error_type': 'invalid_type',
                                    'message': f"{rule_prefix}: relationship at index {rel_idx} must be a dict"
                                })
                                continue

                            # Required relationship fields
                            for rel_field in ['type', 'target', 'rationale']:
                                if rel_field not in rel:
                                    errors.append({
                                        'category': category,
                                        'index': idx,
                                        'field': f'relationships[{rel_idx}].{rel_field}',
                                        'error_type': 'missing_field',
                                        'message': f"{rule_prefix}: relationship missing field '{rel_field}'"
                                    })

                            # Validate relationship type
                            if 'type' in rel:
                                valid_types = ['implements', 'extends', 'conflicts_with', 'related_to']
                                if rel['type'] not in valid_types:
                                    errors.append({
                                        'category': category,
                                        'index': idx,
                                        'field': f'relationships[{rel_idx}].type',
                                        'error_type': 'invalid_value',
                                        'message': f"{rule_prefix}: relationship type '{rel['type']}' not in {valid_types}"
                                    })

                # RREL-009: Validate implementation_refs structure (optional field)
                if 'implementation_refs' in rule:
                    if not isinstance(rule['implementation_refs'], list):
                        errors.append({
                            'category': category,
                            'index': idx,
                            'field': 'implementation_refs',
                            'error_type': 'invalid_type',
                            'message': f"{rule_prefix}: implementation_refs must be a list"
                        })
                    else:
                        for ref_idx, ref in enumerate(rule['implementation_refs']):
                            if not isinstance(ref, dict):
                                errors.append({
                                    'category': category,
                                    'index': idx,
                                    'field': f'implementation_refs[{ref_idx}]',
                                    'error_type': 'invalid_type',
                                    'message': f"{rule_prefix}: implementation_ref at index {ref_idx} must be a dict"
                                })
                                continue

                            # Required implementation_ref fields
                            for ref_field in ['type', 'file', 'role_description']:
                                if ref_field not in ref:
                                    errors.append({
                                        'category': category,
                                        'index': idx,
                                        'field': f'implementation_refs[{ref_idx}].{ref_field}',
                                        'error_type': 'missing_field',
                                        'message': f"{rule_prefix}: implementation_ref missing field '{ref_field}'"
                                    })

                            # Validate implementation_ref type
                            if 'type' in ref:
                                valid_types = ['implements', 'validates', 'documents']
                                if ref['type'] not in valid_types:
                                    errors.append({
                                        'category': category,
                                        'index': idx,
                                        'field': f'implementation_refs[{ref_idx}].type',
                                        'error_type': 'invalid_value',
                                        'message': f"{rule_prefix}: implementation_ref type '{ref['type']}' not in {valid_types}"
                                    })

                            # Validate lines field if present (optional, but must be [start, end] if present)
                            if 'lines' in ref:
                                lines = ref['lines']
                                if not isinstance(lines, list) or len(lines) != 2:
                                    errors.append({
                                        'category': category,
                                        'index': idx,
                                        'field': f'implementation_refs[{ref_idx}].lines',
                                        'error_type': 'invalid_format',
                                        'message': f"{rule_prefix}: lines must be array of 2 integers [start, end]"
                                    })
                                elif not all(isinstance(x, int) for x in lines):
                                    errors.append({
                                        'category': category,
                                        'index': idx,
                                        'field': f'implementation_refs[{ref_idx}].lines',
                                        'error_type': 'invalid_type',
                                        'message': f"{rule_prefix}: lines must contain integers"
                                    })

    # CAP-026: Validate session_context structure
    if 'session_context' in chatlog:
        ctx = chatlog['session_context']
        required_ctx_fields = [
            'problem_solved', 'patterns_applied', 'anti_patterns_avoided',
            'conventions_established', 'reusability_scope'
        ]
        for field in required_ctx_fields:
            if field not in ctx:
                errors.append({
                    'category': 'session_context',
                    'field': field,
                    'error_type': 'missing_field',
                    'message': f"Missing session_context field: {field}"
                })

        # CAP-026b: Validate reusability_scope structure
        if 'reusability_scope' in ctx:
            scope = ctx['reusability_scope']
            for scope_field in ['project_wide', 'module_scoped', 'historical']:
                if scope_field not in scope:
                    errors.append({
                        'category': 'session_context',
                        'field': f'reusability_scope.{scope_field}',
                        'error_type': 'missing_field',
                        'message': f"Missing reusability_scope field: {scope_field}"
                    })

    # CAP-027: Validate artifacts structure
    if 'artifacts' in chatlog:
        artifacts = chatlog['artifacts']
        for field in ['files_modified', 'commands_executed']:
            if field not in artifacts:
                errors.append({
                    'category': 'artifacts',
                    'field': field,
                    'error_type': 'missing_field',
                    'message': f"Missing artifacts field: {field}"
                })

    return errors


def validate_quality(chatlog, config, debug_mode=False):
    """
    Perform quality validation (non-blocking warnings).

    CAP-061 through CAP-065: Quality pattern detection
    CAP-040i, CAP-040j: Debug mode warnings
    """
    warnings = []

    if 'rules' not in chatlog:
        return warnings

    rules = chatlog['rules']
    all_rules = []

    # Collect all rules for aggregate checks
    for category in ['decisions', 'constraints', 'invariants']:
        if category in rules:
            for idx, rule in enumerate(rules[category]):
                all_rules.append({
                    'category': category,
                    'index': idx,
                    'rule': rule
                })

    # CAP-040i: Debug mode - warn about domains not in current vocabulary
    if debug_mode:
        vocabulary_domains = set(config.get('domain_tags', []))
        for item in all_rules:
            rule = item['rule']
            category = item['category']
            idx = item['index']

            if 'domain' in rule and rule['domain'] not in vocabulary_domains:
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': category,
                    'index': idx,
                    'message': f"[MEDIUM] Domain '{rule['domain']}' not in current vocabulary tier-1 domains. May be deprecated or renamed."
                })

    # CAP-040j: Debug mode - report rules below confidence threshold
    if debug_mode:
        threshold = 0.5  # Hardcoded per EXT-021
        low_confidence_rules = [
            item for item in all_rules
            if 'confidence' in item['rule'] and item['rule']['confidence'] < threshold
        ]
        if low_confidence_rules:
            count = len(low_confidence_rules)
            total = len(all_rules)
            percentage = int((count / total) * 100) if total > 0 else 0
            warnings.append({
                'severity': 'INFO',
                'category': 'aggregate',
                'message': f"[INFO] {count} rules ({percentage}%) below confidence threshold {threshold} - will be filtered by extract.py"
            })

    # CAP-061: Multi-behavior pattern detection (constraints only)
    if 'constraints' in rules:
        for idx, rule in enumerate(rules['constraints']):
            topic = rule.get('topic', '')
            rationale = rule.get('rationale', '')
            text = f"{topic} {rationale}"

            # High severity patterns
            if 'and also' in text:
                warnings.append({
                    'severity': 'HIGH',
                    'category': 'constraints',
                    'index': idx,
                    'message': f"[HIGH] Possible multi-behavior CON (contains 'and also'). Consider splitting per INV-002."
                })
            elif 'in addition' in text:
                warnings.append({
                    'severity': 'HIGH',
                    'category': 'constraints',
                    'index': idx,
                    'message': f"[HIGH] Possible multi-behavior CON (contains 'in addition'). Consider splitting per INV-002."
                })
            # Medium severity patterns
            elif '; ' in text:
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': 'constraints',
                    'index': idx,
                    'message': f"[MEDIUM] Possible multi-behavior CON (contains '; '). Review if behaviors are cohesive."
                })
            # Low severity patterns
            elif ' and ' in text:
                warnings.append({
                    'severity': 'LOW',
                    'category': 'constraints',
                    'index': idx,
                    'message': f"[LOW] Possible multi-behavior CON (contains ' and '). Many legitimate uses exist."
                })

    # CAP-062: Temporal language detection (all rule types)
    temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']
    for item in all_rules:
        rule = item['rule']
        category = item['category']
        idx = item['index']

        topic = rule.get('topic', '')
        rationale = rule.get('rationale', '')
        text = f"{topic} {rationale}"

        for pattern in temporal_patterns:
            if pattern in text:
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': category,
                    'index': idx,
                    'message': f"[MEDIUM] Temporal language detected (contains '{pattern.strip()}'). Assess if lifecycle candidate."
                })
                break  # Only report once per rule

    # CAP-063: Cross-domain boundary violations
    for item in all_rules:
        rule = item['rule']
        category = item['category']
        idx = item['index']

        topic = rule.get('topic', '')
        rationale = rule.get('rationale', '')
        text = f"{topic} {rationale}"

        # Detect: mentions 'model/' AND ('build/' OR 'context engine')
        has_model = 'model/' in text
        has_build = 'build/' in text or 'context engine' in text.lower()

        if has_model and has_build:
            warnings.append({
                'severity': 'MEDIUM',
                'category': category,
                'index': idx,
                'message': f"[MEDIUM] Cross-domain boundary violation detected (references both model/ and build/context-engine). See CON-00056."
            })

    return warnings


def main():
    """
    Chatlog validation with schema checks and quality warnings.

    CAP-041: External validation script
    CAP-041a: Debug mode flag support
    CAP-042: JSON output format
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Validate chatlog YAML schema and quality'
    )
    parser.add_argument('chatlog_file', help='Path to chatlog YAML file')
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode (deployment compatibility checks)'
    )
    args = parser.parse_args()

    chatlog_path = Path(args.chatlog_file)
    debug_mode = args.debug

    # Check file exists
    if not chatlog_path.exists():
        result = {
            'valid': False,
            'errors': [{
                'category': 'file',
                'error_type': 'not_found',
                'message': f"File not found: {chatlog_path}"
            }]
        }
        print(json.dumps(result, indent=2))
        return 2

    # Load config
    try:
        config = load_config()
    except Exception as e:
        result = {
            'valid': False,
            'errors': [{
                'category': 'config',
                'error_type': 'load_failed',
                'message': f"Failed to load configuration: {e}"
            }]
        }
        print(json.dumps(result, indent=2))
        return 2

    # Parse YAML
    try:
        with open(chatlog_path) as f:
            chatlog = yaml.safe_load(f)
    except yaml.YAMLError as e:
        result = {
            'valid': False,
            'errors': [{
                'category': 'file',
                'error_type': 'yaml_parse_error',
                'message': f"YAML parse error: {e}"
            }]
        }
        print(json.dumps(result, indent=2))
        return 2

    # CAP-020: Validate it's a dict (not markdown prose)
    if not isinstance(chatlog, dict):
        result = {
            'valid': False,
            'errors': [{
                'category': 'file',
                'error_type': 'invalid_structure',
                'message': "Chatlog must be valid YAML dict, not markdown prose"
            }]
        }
        print(json.dumps(result, indent=2))
        return 1

    # Schema validation (CAP-040 through CAP-040j)
    errors = validate_schema(chatlog, config, debug_mode)

    # CAP-064: Quality warnings are non-blocking
    warnings = []
    if not errors:  # Only check quality if schema is valid
        warnings = validate_quality(chatlog, config, debug_mode)

    # CAP-042: Return JSON result
    result = {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }

    # Print JSON to stdout
    print(json.dumps(result, indent=2))

    # CAP-041a: Exit codes
    # 0: Valid (no errors, may have warnings)
    # 1: Invalid (schema errors or debug-mode version mismatch)
    # 2: File not found or YAML parse error
    return 0 if result['valid'] else 1


if __name__ == '__main__':
    sys.exit(main())
