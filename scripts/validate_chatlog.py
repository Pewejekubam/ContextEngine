#!/usr/bin/env python3
"""
Chatlog validation script with schema and quality checks

Implements constraints: CAP-001 through CAP-090
Generated from: specs/modules/runtime-command-chatlog-capture-v1.14.0.yaml
"""

import sys
import json
import argparse
import re
import uuid
from datetime import datetime
from pathlib import Path
from difflib import get_close_matches

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml


def find_config():
    """Find deployment.yaml in common locations."""
    config_locations = [
        Path.cwd() / '.context-engine' / 'config' / 'deployment.yaml',
        Path.home() / '.context-engine' / 'config' / 'deployment.yaml',
    ]

    for config_path in config_locations:
        if config_path.exists():
            return config_path

    # Fallback for when script is run from within .context-engine
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent
    fallback = base_dir / "config" / "deployment.yaml"
    if fallback.exists():
        return fallback

    return None


def load_config(debug_mode=False):
    """Load deployment configuration and vocabulary.

    Implements:
    - CAP-066: Loads vocabulary file from deployment config
    - CAP-072: Runtime validator loads domains from vocabulary
    """
    config_path = find_config()
    if not config_path:
        print("Error: Could not find deployment.yaml", file=sys.stderr)
        sys.exit(2)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # CAP-066: Load tag vocabulary
    base_dir = Path(config['paths']['context_engine_home'])
    vocab_path = base_dir / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')

    if not vocab_path.exists():
        print(f"Error: Vocabulary file not found: {vocab_path}", file=sys.stderr)
        sys.exit(2)

    with open(vocab_path) as f:
        vocabulary = yaml.safe_load(f)

    # CAP-066/CAP-067: Extract domains with hyphen->underscore normalization
    config['domain_tags'] = [
        d.replace('-', '_') for d in vocabulary.get('tier_1_domains', {}).keys()
    ]

    # Store schema version for debug mode validation
    if debug_mode:
        config['deployed_schema_version'] = config.get('behavior', {}).get('chatlog_schema_version', 'v1.0.0')

    return config


def validate_schema(chatlog, config, debug_mode=False):
    """Validate chatlog schema structure.

    Implements:
    - CAP-040a through CAP-040g: Schema validation
    - CAP-040h through CAP-040j: Debug mode compatibility checks
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
        return errors, warnings

    # CAP-040b: Validate UUID v4
    try:
        parsed_uuid = uuid.UUID(chatlog['chatlog_id'], version=4)
        if str(parsed_uuid) != chatlog['chatlog_id']:
            errors.append({
                'category': 'top_level',
                'field': 'chatlog_id',
                'error_type': 'invalid_format',
                'message': f"Invalid UUID format: {chatlog['chatlog_id']}"
            })
    except (ValueError, AttributeError):
        errors.append({
            'category': 'top_level',
            'field': 'chatlog_id',
            'error_type': 'invalid_format',
            'message': f"Invalid UUID v4: {chatlog.get('chatlog_id', 'missing')}"
        })

    # CAP-040c: Validate ISO 8601 UTC timestamp
    timestamp = chatlog.get('timestamp', '')
    iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
    if not re.match(iso_pattern, timestamp):
        errors.append({
            'category': 'top_level',
            'field': 'timestamp',
            'error_type': 'invalid_format',
            'message': f"Invalid ISO 8601 UTC timestamp (must end with Z): {timestamp}"
        })

    # CAP-040h: Debug mode - schema version compatibility
    if debug_mode:
        deployed_version = config.get('deployed_schema_version', '')
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
            'field': 'rules',
            'error_type': 'invalid_type',
            'message': "Rules must be a dictionary"
        })
        return errors, warnings

    # CAP-040d, CAP-040e: Validate each rule category
    domain_tags = config.get('domain_tags', [])
    vocabulary_domains = set(domain_tags)

    all_rules = []
    for category in ['decisions', 'constraints', 'invariants']:
        category_rules = rules.get(category, [])
        if not isinstance(category_rules, list):
            errors.append({
                'category': category,
                'field': category,
                'error_type': 'invalid_type',
                'message': f"{category} must be a list"
            })
            continue

        for idx, rule in enumerate(category_rules):
            all_rules.append(rule)

            # CAP-023: Required fields for all rules
            rule_required = ['topic', 'rationale', 'domain', 'confidence']
            for field in rule_required:
                if field not in rule:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': field,
                        'error_type': 'missing_field',
                        'message': f"Rule at {category} index {idx}: missing {field}"
                    })

            # CAP-040d: Domain validation
            if 'domain' in rule:
                if rule['domain'] not in domain_tags:
                    errors.append({
                        'category': category,
                        'index': idx,
                        'field': 'domain',
                        'error_type': 'invalid_value',
                        'message': f"Rule at {category} index {idx}: domain '{rule['domain']}' not in allowed list {domain_tags}",
                        'current_value': rule['domain']
                    })

                # CAP-040i: Warn about domains not in vocabulary (debug mode)
                if debug_mode and rule['domain'] not in vocabulary_domains:
                    warnings.append({
                        'severity': 'MEDIUM',
                        'category': category,
                        'index': idx,
                        'message': f"[MEDIUM] Domain '{rule['domain']}' not in current vocabulary tier-1 domains"
                    })

            # CAP-040e: Confidence validation
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
                            'current_value': conf
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
                            'message': f"Rule at {category} index {idx}: missing decision field {field}"
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

    # CAP-040j: Report low confidence rules (debug mode)
    if debug_mode and all_rules:
        threshold = 0.5
        low_conf_rules = [r for r in all_rules if r.get('confidence', 1.0) < threshold]
        if low_conf_rules:
            percentage = (len(low_conf_rules) / len(all_rules)) * 100
            warnings.append({
                'severity': 'INFO',
                'message': f"[INFO] {len(low_conf_rules)} rules ({percentage:.0f}%) below confidence threshold {threshold} - will be filtered by extract.py"
            })

    # Validate session_context structure
    session_context = chatlog.get('session_context', {})
    if not isinstance(session_context, dict):
        errors.append({
            'category': 'session_context',
            'field': 'session_context',
            'error_type': 'invalid_type',
            'message': "session_context must be a dictionary"
        })
    else:
        # CAP-026: Required session_context fields
        context_required = ['problem_solved', 'patterns_applied', 'anti_patterns_avoided',
                           'conventions_established', 'reusability_scope']
        for field in context_required:
            if field not in session_context:
                errors.append({
                    'category': 'session_context',
                    'field': field,
                    'error_type': 'missing_field',
                    'message': f"session_context missing {field}"
                })

        # CAP-026b: Validate reusability_scope structure
        reusability_scope = session_context.get('reusability_scope', {})
        if isinstance(reusability_scope, dict):
            scope_fields = ['project_wide', 'module_scoped', 'historical']
            for field in scope_fields:
                if field not in reusability_scope:
                    errors.append({
                        'category': 'session_context',
                        'field': f'reusability_scope.{field}',
                        'error_type': 'missing_field',
                        'message': f"reusability_scope missing {field}"
                    })

    # CAP-027: Validate artifacts structure
    artifacts = chatlog.get('artifacts', {})
    if not isinstance(artifacts, dict):
        errors.append({
            'category': 'artifacts',
            'field': 'artifacts',
            'error_type': 'invalid_type',
            'message': "artifacts must be a dictionary"
        })
    else:
        artifact_fields = ['files_modified', 'commands_executed']
        for field in artifact_fields:
            if field not in artifacts:
                errors.append({
                    'category': 'artifacts',
                    'field': field,
                    'error_type': 'missing_field',
                    'message': f"artifacts missing {field}"
                })

    return errors, warnings


def validate_quality(chatlog, config):
    """Run quality checks for non-blocking warnings.

    Implements:
    - CAP-061: Multi-behavior detection
    - CAP-062: Temporal language detection
    - CAP-063: Cross-domain boundary violations
    - CAP-064/CAP-065: Non-blocking warnings with severity
    """
    warnings = []

    rules = chatlog.get('rules', {})

    for category in ['decisions', 'constraints', 'invariants']:
        category_rules = rules.get(category, [])

        for idx, rule in enumerate(category_rules):
            topic = rule.get('topic', '')
            rationale = rule.get('rationale', '')
            combined_text = f"{topic} {rationale}"

            # CAP-061: Multi-behavior detection
            if 'and also' in combined_text.lower():
                warnings.append({
                    'severity': 'HIGH',
                    'category': category,
                    'index': idx,
                    'message': f"[HIGH] Possible multi-behavior {category.upper()[:-1]} (contains 'and also'). Consider splitting per INV-002."
                })
            elif 'in addition' in combined_text.lower():
                warnings.append({
                    'severity': 'HIGH',
                    'category': category,
                    'index': idx,
                    'message': f"[HIGH] Possible multi-behavior {category.upper()[:-1]} (contains 'in addition'). Consider splitting per INV-002."
                })
            elif '; ' in combined_text:
                warnings.append({
                    'severity': 'MEDIUM',
                    'category': category,
                    'index': idx,
                    'message': f"[MEDIUM] Possible multi-behavior {category.upper()[:-1]} (contains '; '). Review if single behavior."
                })
            elif ' and ' in combined_text:
                warnings.append({
                    'severity': 'LOW',
                    'category': category,
                    'index': idx,
                    'message': f"[LOW] Possible multi-behavior {category.upper()[:-1]} (contains ' and '). Often false positive - review context."
                })

            # CAP-062: Temporal language detection
            temporal_patterns = ['was ', 'were ', 'Phase ', 'completed', 'during ', 'after ']
            for pattern in temporal_patterns:
                if pattern in combined_text:
                    warnings.append({
                        'severity': 'MEDIUM',
                        'category': category,
                        'index': idx,
                        'message': f"[MEDIUM] Temporal language detected (contains '{pattern.strip()}'). May indicate lifecycle candidate."
                    })
                    break  # Only warn once per rule

            # CAP-063: Cross-domain boundary violations
            if 'model/' in combined_text and ('build/' in combined_text or 'context engine' in combined_text.lower()):
                warnings.append({
                    'severity': 'HIGH',
                    'category': category,
                    'index': idx,
                    'message': f"[HIGH] Cross-domain boundary violation (System Domain references Build Domain). See CON-00056."
                })

    return warnings


def apply_remediation(chatlog, errors, config):
    """Apply automatic fixes to common errors.

    Implements CAP-089: Six remediation patterns
    """
    fixes_applied = []
    modified = False

    domain_tags = config.get('domain_tags', [])
    rules = chatlog.get('rules', {})

    for error in errors:
        category = error.get('category')
        index = error.get('index')
        field = error.get('field')
        error_type = error.get('error_type')

        # Pattern 1: FUZZY_MATCH_DOMAIN
        if error_type == 'invalid_value' and field == 'domain':
            if category in rules and index is not None:
                current = error.get('current_value', '')
                matches = get_close_matches(current, domain_tags, n=1, cutoff=0.6)
                if matches:
                    rules[category][index]['domain'] = matches[0]
                    fixes_applied.append({
                        'pattern': 'FUZZY_MATCH_DOMAIN',
                        'field': f'rules.{category}[{index}].domain',
                        'old': current,
                        'new': matches[0]
                    })
                    modified = True

        # Pattern 2: CLAMP_CONFIDENCE
        elif error_type == 'out_of_range' and field == 'confidence':
            if category in rules and index is not None:
                current = error.get('current_value', 0.5)
                if current > 1.0:
                    new_value = 0.95
                elif current < 0.0:
                    new_value = 0.5
                else:
                    new_value = 0.5
                rules[category][index]['confidence'] = new_value
                fixes_applied.append({
                    'pattern': 'CLAMP_CONFIDENCE',
                    'field': f'rules.{category}[{index}].confidence',
                    'old': current,
                    'new': new_value
                })
                modified = True

        # Pattern 3: REGENERATE_UUID
        elif error_type == 'invalid_format' and field == 'chatlog_id':
            new_uuid = str(uuid.uuid4())
            chatlog['chatlog_id'] = new_uuid
            fixes_applied.append({
                'pattern': 'REGENERATE_UUID',
                'field': 'chatlog_id',
                'old': chatlog.get('chatlog_id', 'invalid'),
                'new': new_uuid
            })
            modified = True

        # Pattern 4: REGENERATE_TIMESTAMP
        elif error_type == 'invalid_format' and field == 'timestamp':
            new_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            chatlog['timestamp'] = new_timestamp
            fixes_applied.append({
                'pattern': 'REGENERATE_TIMESTAMP',
                'field': 'timestamp',
                'old': chatlog.get('timestamp', 'invalid'),
                'new': new_timestamp
            })
            modified = True

        # Pattern 5: ADD_VALIDATION_METHOD
        elif error_type == 'missing_field' and field == 'validation_method':
            if category == 'constraints' and index is not None:
                rules['constraints'][index]['validation_method'] = 'Code review required'
                fixes_applied.append({
                    'pattern': 'ADD_VALIDATION_METHOD',
                    'field': f'rules.constraints[{index}].validation_method',
                    'old': None,
                    'new': 'Code review required'
                })
                modified = True

        # Pattern 6: ADD_REUSABILITY_SCOPE_FIELDS
        elif error_type == 'missing_field' and 'reusability_scope' in field:
            session_context = chatlog.get('session_context', {})
            if 'reusability_scope' not in session_context:
                session_context['reusability_scope'] = {}

            reusability_scope = session_context['reusability_scope']
            if 'project_wide' not in reusability_scope:
                reusability_scope['project_wide'] = []
            if 'module_scoped' not in reusability_scope:
                reusability_scope['module_scoped'] = {}
            if 'historical' not in reusability_scope:
                reusability_scope['historical'] = []

            fixes_applied.append({
                'pattern': 'ADD_REUSABILITY_SCOPE_FIELDS',
                'field': 'session_context.reusability_scope',
                'old': None,
                'new': 'Added missing subfields'
            })
            modified = True

    return chatlog, fixes_applied, modified


def main():
    """Chatlog validation with optional remediation.

    Implements:
    - CAP-087: --remediate flag for validation + auto-fix
    - CAP-088: Structured JSON output
    - CAP-041a: --debug flag for deployment compatibility
    """
    parser = argparse.ArgumentParser(
        description='Validate chatlog YAML files with optional remediation'
    )
    parser.add_argument('chatlog_file', help='Path to chatlog YAML file')
    parser.add_argument('--remediate', action='store_true',
                       help='Enable automatic remediation of common errors')
    parser.add_argument('--max-attempts', type=int, default=3,
                       help='Maximum remediation attempts (default: 3)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable deployment compatibility checks (CAP-040h/i/j)')

    args = parser.parse_args()

    chatlog_path = Path(args.chatlog_file)
    if not chatlog_path.exists():
        result = {
            'valid': False,
            'errors': [{'message': f"File not found: {chatlog_path}"}]
        }
        print(json.dumps(result, indent=2))
        return 2

    # Load configuration
    try:
        config = load_config(debug_mode=args.debug)
    except Exception as e:
        result = {
            'valid': False,
            'errors': [{'message': f"Error loading configuration: {e}"}]
        }
        print(json.dumps(result, indent=2))
        return 2

    # Load chatlog
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

    if args.remediate:
        # CAP-087: Remediation mode with iteration
        attempt = 0
        all_fixes = []

        while attempt < args.max_attempts:
            attempt += 1

            # Validate
            errors, warnings = validate_schema(chatlog, config, debug_mode=args.debug)

            if not errors:
                # Add quality warnings
                quality_warnings = validate_quality(chatlog, config)
                warnings.extend(quality_warnings)

                # Success
                result = {
                    'success': True,
                    'valid': True,
                    'file': str(chatlog_path.absolute()),
                    'attempts': attempt,
                    'fixes_applied': all_fixes,
                    'warnings': warnings,
                    'errors': []
                }

                # Write back modified chatlog if fixes were applied
                if all_fixes:
                    with open(chatlog_path, 'w') as f:
                        yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False)

                print(json.dumps(result, indent=2))
                return 0

            # Apply remediation
            chatlog, fixes, modified = apply_remediation(chatlog, errors, config)
            all_fixes.extend(fixes)

            if not modified:
                # No fixes possible, exit
                break

        # Failed after max attempts - save as .invalid
        invalid_path = chatlog_path.with_suffix('.invalid')
        with open(invalid_path, 'w') as f:
            f.write(f"# VALIDATION ERRORS:\n")
            for error in errors:
                f.write(f"# - {error.get('message', 'Unknown error')}\n")
            f.write("\n")
            yaml.dump(chatlog, f, default_flow_style=False, sort_keys=False)

        result = {
            'success': False,
            'valid': False,
            'file': str(chatlog_path.absolute()),
            'attempts': attempt,
            'fixes_applied': all_fixes,
            'warnings': warnings,
            'errors': errors
        }
        print(json.dumps(result, indent=2))
        return 1

    else:
        # CAP-042: Simple validation mode
        errors, warnings = validate_schema(chatlog, config, debug_mode=args.debug)

        if not errors:
            # Add quality warnings
            quality_warnings = validate_quality(chatlog, config)
            warnings.extend(quality_warnings)

            result = {
                'valid': True,
                'warnings': warnings
            }
            print(json.dumps(result, indent=2))
            return 0
        else:
            result = {
                'valid': False,
                'errors': errors,
                'warnings': warnings
            }
            print(json.dumps(result, indent=2))
            return 1


if __name__ == '__main__':
    sys.exit(main())
