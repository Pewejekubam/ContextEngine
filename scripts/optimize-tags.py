#!/usr/bin/env python3
"""
Tag optimization with vocabulary-aware intelligence and HITL workflow

Implements constraints: OPT-001 through OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.10.yaml
"""

import sys
import json
import sqlite3
import subprocess
import argparse
import re
import fcntl
import time
import random
from pathlib import Path
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def load_vocabulary(vocab_path):
    """OPT-019: Load vocabulary from tag-vocabulary.yaml."""
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab
    except Exception as e:
        # OPT-035, OPT-035a: Vocabulary load failure aborts with error
        print(f"Error loading vocabulary from {vocab_path}: {e}", file=sys.stderr)
        sys.exit(1)


def get_tier_1_domains(vocab_path):
    """OPT-060a: Extract tier-1 domain names from vocabulary file."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    tier_1_domains = list(vocab.get('tier_1_domains', {}).keys())
    return tier_1_domains


def load_all_tier2_tags_from_vocabulary(vocab_path):
    """OPT-062a: Load all tier-2 tags from vocabulary file across all domains."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    all_tags = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        all_tags.extend(tags)
    return all_tags


def format_vocabulary_for_prompt(vocab):
    """OPT-034c-034f: Format vocabulary components for prompt template."""
    # OPT-034c: Tier-1 domains as comma-separated list
    tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

    # OPT-034d: Tier-2 tags with ellipsis notation
    tier_2_tags_lines = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if len(tags) <= 10:
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
        else:
            remaining = len(tags) - 10
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
    tier_2_tags = '\n'.join(tier_2_tags_lines)

    # OPT-034e: Vocabulary mappings (first 5 examples)
    mappings = vocab.get('vocabulary_mappings', {})
    if mappings and len(mappings) > 0:
        mapping_items = list(mappings.items())[:5]
        mapping_lines = [f'  "{word}" → {canonical}' for word, canonical in mapping_items]
        if len(mappings) > 5:
            mapping_lines.append(f"  ... (and {len(mappings) - 5} more)")
        vocabulary_mappings = '\n'.join(mapping_lines)
    else:
        vocabulary_mappings = '  (none defined)'

    # Synonyms
    synonyms_content = vocab.get('synonyms', {})
    if synonyms_content:
        synonym_lines = [f"  {canonical}: {', '.join(variants)}" for canonical, variants in list(synonyms_content.items())[:5]]
        if len(synonyms_content) > 5:
            synonym_lines.append(f"  ... (and {len(synonyms_content) - 5} more)")
        synonyms = '\n'.join(synonym_lines)
    else:
        synonyms = '  (none defined)'

    # OPT-034f: Forbidden stopwords (first 20 words)
    stopwords = vocab.get('forbidden_tags', [])
    if len(stopwords) <= 20:
        forbidden_stopwords = ', '.join(stopwords)
    else:
        forbidden_stopwords = ', '.join(stopwords[:20]) + f", ... (and {len(stopwords) - 20} more)"

    return {
        'tier_1_domains': tier_1_domains,
        'tier_2_tags': tier_2_tags,
        'vocabulary_mappings': vocabulary_mappings,
        'synonyms': synonyms,
        'forbidden_stopwords': forbidden_stopwords
    }


def validate_response(response_data, vocab, rule_domain):
    """OPT-029-033b: Validate Claude response against vocabulary constraints."""
    errors = []

    # OPT-030: Validate tag count (2-5)
    tags = response_data.get('tags', [])
    if not isinstance(tags, list) or len(tags) < 2 or len(tags) > 5:
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Validate against forbidden stopwords
    forbidden = vocab.get('forbidden_tags', [])
    for tag in tags:
        if tag in forbidden:
            errors.append(f"forbidden tag '{tag}'")

    # OPT-031: Validate domain exists in tier_1_domains
    domain = response_data.get('domain', rule_domain)
    if domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"invalid domain '{domain}'")

    # OPT-032, OPT-032a: Validate confidence score
    confidence = response_data.get('confidence')
    if confidence is None:
        response_data['confidence'] = 0.5
    elif not isinstance(confidence, (int, float)) or confidence < 0.0 or confidence > 1.0:
        response_data['confidence'] = 0.5

    return errors


def calculate_coherence(proposed_tags, domain_vocab):
    """OPT-050: Calculate coherence as precision (intersection / proposed)."""
    if not proposed_tags:
        return 0.0

    intersection = sum(1 for tag in proposed_tags if tag in domain_vocab)
    precision = intersection / len(proposed_tags)
    return precision


def call_claude_cli(rule, template, vocab_formatted):
    """OPT-036, OPT-037: Invoke Claude CLI and parse response."""
    # Prepare prompt
    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'] or '',
        domain=rule['domain'] or '(unspecified)',
        tags=', '.join(json.loads(rule['tags'] or '[]')) or '(none)',
        tier_1_domains=vocab_formatted['tier_1_domains'],
        tier_2_tags=vocab_formatted['tier_2_tags'],
        vocabulary_mappings=vocab_formatted['vocabulary_mappings'],
        synonyms=vocab_formatted['synonyms'],
        forbidden_stopwords=vocab_formatted['forbidden_stopwords'],
        session_context=''
    )

    try:
        # OPT-036: Invoke Claude CLI with prompt as argument (not stdin)
        result = subprocess.run(
            ['claude', '--print', prompt],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL
        )

        if result.returncode != 0:
            return {'status': 'error', 'error': f"CLI failed: {result.stderr[:200]}"}

    except subprocess.TimeoutExpired:
        return {'status': 'error', 'error': 'CLI timeout'}
    except FileNotFoundError:
        print("Error: 'claude' command not found. Install Claude CLI first.", file=sys.stderr)
        sys.exit(1)

    # OPT-037b: Extract JSON from markdown code blocks
    raw_response = result.stdout.strip()
    json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', raw_response, re.DOTALL)
    if json_match:
        json_str = json_match.group(1).strip()
    else:
        json_str = raw_response

    try:
        response_data = json.loads(json_str)
        return {'status': 'success', 'data': response_data, 'raw': raw_response}
    except json.JSONDecodeError as e:
        return {'status': 'parse_error', 'error': str(e), 'raw': raw_response[:500]}


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """OPT-039-041a: Update vocabulary with approved tags (thread-safe)."""
    # OPT-041a: Use exclusive file locking
    with open(vocab_path, 'r+') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

        vocab = yaml.safe_load(f)
        if vocab is None:
            return

        # OPT-039a: Validate domain exists in tier_1_domains
        if rule_domain not in vocab.get('tier_1_domains', {}):
            # OPT-039c: Log warning
            log_path = BASE_DIR / 'data' / 'tag_optimization_warnings.log'
            log_path.parent.mkdir(exist_ok=True)
            timestamp = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
            with open(log_path, 'a') as log:
                log.write(f"{timestamp}\t{rule_id}\tinvalid_domain\t{rule_domain}\tskipped_vocabulary_update\n")
            return

        # OPT-039b: Ensure tier_2_tags entry exists
        if 'tier_2_tags' not in vocab:
            vocab['tier_2_tags'] = {}
        if rule_domain not in vocab['tier_2_tags']:
            vocab['tier_2_tags'][rule_domain] = []

        # OPT-040: Append only if not already present
        tags_added = False
        for tag in approved_tags:
            if tag not in vocab['tier_2_tags'][rule_domain]:
                vocab['tier_2_tags'][rule_domain].append(tag)
                tags_added = True

        # OPT-041: Save with block style, preserve insertion order
        if tags_added:
            f.seek(0)
            f.truncate()
            yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)


def process_rule_interactive(rule, template, vocab, vocab_formatted, db_path):
    """Process single rule in interactive mode (OPT-009, OPT-010)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Call Claude CLI
    cli_result = call_claude_cli(rule, template, vocab_formatted)

    if cli_result['status'] == 'error':
        # OPT-036a: Store error in metadata
        error_metadata = json.loads(rule['metadata'] or '{}')
        error_metadata['optimization_error'] = cli_result['error']
        conn.execute("UPDATE rules SET metadata = ? WHERE id = ?",
                    (json.dumps(error_metadata), rule['id']))
        conn.commit()
        conn.close()
        print(f"✗ Error processing {rule['id']}: {cli_result['error']}", file=sys.stderr)
        return {'status': 'error'}

    if cli_result['status'] == 'parse_error':
        # OPT-037a: Store raw response in metadata
        failure_metadata = json.loads(rule['metadata'] or '{}')
        failure_metadata['parse_failure'] = cli_result['raw']
        conn.execute("UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                    ('pending_review', json.dumps(failure_metadata), rule['id']))
        conn.commit()
        conn.close()
        print(f"✗ Parse error for {rule['id']}: {cli_result['error']}", file=sys.stderr)
        return {'status': 'error'}

    response_data = cli_result['data']

    # Validate response
    validation_errors = validate_response(response_data, vocab, rule['domain'])
    if validation_errors:
        # OPT-033, OPT-033a, OPT-033b: Validation failure
        error_metadata = json.loads(rule['metadata'] or '{}')
        error_metadata['validation_failure'] = '; '.join(validation_errors)
        conn.execute("UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                    ('pending_review', json.dumps(error_metadata), rule['id']))
        conn.commit()
        conn.close()
        print(f"✗ Validation failed for {rule['id']}: {'; '.join(validation_errors)}", file=sys.stderr)
        return {'status': 'validation_failed', 'error': '; '.join(validation_errors)}

    # Extract response fields
    suggested_tags = response_data.get('tags', [])
    suggested_domain = response_data.get('domain', rule['domain'])
    confidence = response_data.get('confidence', 0.5)
    reasoning = response_data.get('reasoning', '')

    # OPT-010, OPT-010b: Present before/after comparison
    print(f"\nRule: {rule['id']}")
    print(f"Title: {rule['title']}")
    print(f"Domain: {suggested_domain}")
    print(f"Suggested tags: {', '.join(suggested_tags)}")
    print(f"Confidence: {confidence:.2f}")
    print(f"Reasoning: {reasoning}")
    print("\nOptions:")
    print("  1. Approve")
    print("  2. Skip")
    print("  3. Quit")

    while True:
        choice = input("\nYour choice (1-3): ").strip()
        if choice == '1':
            decision = 'approve'
            break
        elif choice == '2':
            decision = 'skip'
            break
        elif choice == '3':
            # OPT-010a: Quit requires confirmation
            confirm = input("Are you sure you want to quit? (y/n): ").strip().lower()
            if confirm == 'y':
                conn.close()
                return {'status': 'quit'}
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    if decision == 'approve':
        # OPT-028: Determine tags_state based on confidence
        if confidence >= 0.9:
            tags_state = 'curated'
        elif confidence >= 0.7:
            tags_state = 'refined'
        else:
            tags_state = 'pending_review'

        # OPT-028e, OPT-028f: Update metadata
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['optimization_reasoning'] = reasoning
        metadata['tag_confidence'] = confidence
        metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

        # OPT-003: Update rule
        conn.execute("""
            UPDATE rules
            SET tags = ?, domain = ?, tags_state = ?, confidence = ?, metadata = ?,
                curated_at = ?, curated_by = ?
            WHERE id = ?
        """, (json.dumps(suggested_tags), suggested_domain, tags_state, confidence,
              json.dumps(metadata), datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
              "Claude Sonnet 4.5", rule['id']))
        conn.commit()

        # OPT-039: Update vocabulary
        vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'
        update_vocabulary(rule['id'], suggested_domain, suggested_tags, vocab_path)

        conn.close()
        return {'status': 'approved', 'tags': suggested_tags, 'confidence': confidence}
    else:
        conn.close()
        return {'status': 'skipped'}


def process_rule_auto(rule, template, vocab, vocab_formatted, db_path, confidence_threshold):
    """Process single rule in auto-approve mode (OPT-011, OPT-044c)."""
    # OPT-044c: Create thread-local connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Call Claude CLI
    cli_result = call_claude_cli(rule, template, vocab_formatted)

    if cli_result['status'] == 'error':
        error_metadata = json.loads(rule['metadata'] or '{}')
        error_metadata['optimization_error'] = cli_result['error']
        conn.execute("UPDATE rules SET metadata = ? WHERE id = ?",
                    (json.dumps(error_metadata), rule['id']))
        conn.commit()
        conn.close()
        return {'status': 'error', 'rule_id': rule['id'], 'error': cli_result['error']}

    if cli_result['status'] == 'parse_error':
        failure_metadata = json.loads(rule['metadata'] or '{}')
        failure_metadata['parse_failure'] = cli_result['raw']
        conn.execute("UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                    ('pending_review', json.dumps(failure_metadata), rule['id']))
        conn.commit()
        conn.close()
        return {'status': 'error', 'rule_id': rule['id'], 'error': cli_result['error']}

    response_data = cli_result['data']

    # Validate response
    validation_errors = validate_response(response_data, vocab, rule['domain'])
    if validation_errors:
        error_metadata = json.loads(rule['metadata'] or '{}')
        error_metadata['validation_failure'] = '; '.join(validation_errors)
        conn.execute("UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                    ('pending_review', json.dumps(error_metadata), rule['id']))
        conn.commit()
        conn.close()
        return {'status': 'error', 'rule_id': rule['id'], 'error': '; '.join(validation_errors)}

    # Extract response fields
    suggested_tags = response_data.get('tags', [])
    suggested_domain = response_data.get('domain', rule['domain'])
    confidence = response_data.get('confidence', 0.5)
    reasoning = response_data.get('reasoning', '')

    # OPT-050, OPT-051, OPT-052: Calculate coherence
    domain_vocab = vocab.get('tier_2_tags', {}).get(suggested_domain, [])
    coherence = calculate_coherence(suggested_tags, domain_vocab)

    # Bootstrap exception: bypass coherence if domain has < 5 tags
    if len(domain_vocab) >= 5 and coherence < 0.3:
        conn.close()
        return {
            'status': 'skipped',
            'rule_id': rule['id'],
            'tags': suggested_tags,
            'confidence': confidence,
            'coherence': coherence,
            'reasoning': reasoning,
            'skip_reason': 'coherence'
        }

    # OPT-011: Auto-approve decision (uniform 0.70 threshold)
    if confidence >= confidence_threshold and coherence >= 0.3:
        decision = 'approve'
    else:
        decision = 'skip'
        conn.close()
        return {
            'status': 'skipped',
            'rule_id': rule['id'],
            'tags': suggested_tags,
            'confidence': confidence,
            'coherence': coherence,
            'reasoning': reasoning,
            'skip_reason': 'confidence' if confidence < confidence_threshold else 'coherence'
        }

    # Approve: Update database
    if confidence >= 0.9:
        tags_state = 'curated'
    elif confidence >= 0.7:
        tags_state = 'refined'
    else:
        tags_state = 'pending_review'

    metadata = json.loads(rule['metadata'] or '{}')
    metadata['optimization_reasoning'] = reasoning
    metadata['tag_confidence'] = confidence
    metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    conn.execute("""
        UPDATE rules
        SET tags = ?, domain = ?, tags_state = ?, confidence = ?, metadata = ?,
            curated_at = ?, curated_by = ?
        WHERE id = ?
    """, (json.dumps(suggested_tags), suggested_domain, tags_state, confidence,
          json.dumps(metadata), datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
          "Claude Sonnet 4.5", rule['id']))
    conn.commit()
    conn.close()

    # OPT-039: Update vocabulary
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'
    update_vocabulary(rule['id'], suggested_domain, suggested_tags, vocab_path)

    return {
        'status': 'approved',
        'rule_id': rule['id'],
        'tags': suggested_tags,
        'confidence': confidence,
        'coherence': coherence,
        'reasoning': reasoning
    }


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, confidence_threshold, prev_avg_confidence):
    """OPT-057a: Execute single optimization pass, return convergence metrics."""
    # Load template
    template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'
    with open(template_path) as f:
        template = f.read()

    # Load vocabulary
    vocab = load_vocabulary(vocab_path)
    vocab_formatted = format_vocabulary_for_prompt(vocab)

    # OPT-062: Track vocabulary before pass
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # Get database path
    db_path = BASE_DIR / config['paths']['database']

    # OPT-044a, OPT-044b: Parallel processing with ThreadPoolExecutor
    max_workers = config.get('tag_optimization', {}).get('max_workers', 3)

    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    print(f"\nPass {pass_number + 1}: Processing {len(remaining_rules)} rules...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_rule_auto, rule, template, vocab, vocab_formatted,
                          db_path, confidence_threshold): rule
            for rule in remaining_rules
        }

        completed = 0
        for future in as_completed(futures):
            rule = futures[future]
            completed += 1

            try:
                result = future.result()

                # OPT-044d: Verbose progress output
                status_icon = {'approved': '✓', 'skipped': '⊘', 'error': '✗'}.get(result['status'], '?')

                print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                print(f"    Title: {rule['title']}")

                if result['status'] == 'approved':
                    print(f"    Confidence: {result['confidence']:.2f} | Coherence: {result['coherence']:.2f}")
                    print(f"    Decision: approved")
                    print(f"    Approved Tags: {', '.join(result['tags'])}")
                    approved_rules.append(result)
                    approved_confidences.append(result['confidence'])
                elif result['status'] == 'skipped':
                    print(f"    Confidence: {result['confidence']:.2f} | Coherence: {result.get('coherence', 0.0):.2f}")
                    if result.get('skip_reason') == 'confidence':
                        print(f"    Decision: skipped (confidence < {confidence_threshold})")
                    elif result.get('skip_reason') == 'coherence':
                        print(f"    Decision: skipped (coherence < 0.3)")
                    else:
                        print(f"    Decision: skipped")
                    print(f"    Suggested Tags: {', '.join(result['tags'])}")
                    skipped_count += 1
                else:
                    print(f"    Decision: error")
                    if result.get('error'):
                        print(f"    Error: {result['error']}")
                    error_count += 1

                # Show reasoning (multi-line support)
                if result.get('reasoning'):
                    reasoning_lines = result['reasoning'].split('\n')
                    print(f"    Reasoning: {reasoning_lines[0]}")
                    for line in reasoning_lines[1:]:
                        if line.strip():
                            print(f"               {line}")

            except Exception as e:
                print(f"✗ Exception processing {rule['id']}: {e}", file=sys.stderr)
                error_count += 1

    # OPT-062: Calculate vocabulary growth
    tags_after = set(load_all_tier2_tags_from_vocabulary(vocab_path))
    new_tags_added = len(tags_after - tags_before)

    # OPT-059: Calculate global improvement rate
    improvement_rate = len(approved_rules) / len(remaining_rules) if remaining_rules else 0.0

    # OPT-060: Domain-specific metrics
    tier_1_domains = get_tier_1_domains(vocab_path)
    approved_rule_ids = {r['rule_id'] for r in approved_rules}
    domain_metrics = {}

    for domain in tier_1_domains:
        domain_rules = [r for r in remaining_rules if r['domain'] == domain]
        domain_approved_count = sum(1 for r in domain_rules if r['id'] in approved_rule_ids)

        if domain_rules:
            domain_metrics[domain] = {
                'total': len(domain_rules),
                'approved': domain_approved_count,
                'improvement_rate': domain_approved_count / len(domain_rules)
            }

    # OPT-061: Domain-level convergence
    any_domain_active = any(metrics['improvement_rate'] > 0.10 for metrics in domain_metrics.values())

    # OPT-064: Average confidence
    avg_confidence = sum(approved_confidences) / len(approved_confidences) if approved_confidences else 0.0

    # OPT-065: Quality degradation detection
    quality_floor_reached = False
    if pass_number > 0 and prev_avg_confidence > 0:
        confidence_drop = prev_avg_confidence - avg_confidence
        if confidence_drop > 0.15:
            print(f"\n⚠️  Warning: Confidence dropped by {confidence_drop:.2f} from previous pass")
        if avg_confidence < 0.65:
            print(f"\n⚠️  Warning: Quality floor reached (avg confidence {avg_confidence:.2f} < 0.65)")
            quality_floor_reached = True

    # OPT-063: Vocabulary saturation
    vocabulary_saturated = (new_tags_added < 3 and improvement_rate < 0.10)

    # Get remaining count
    conn = sqlite3.connect(str(db_path))
    remaining_count = conn.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'").fetchone()[0]
    conn.close()

    # OPT-049: Pass summary
    print(f"\n{'='*70}")
    print(f"Pass {pass_number + 1} Summary")
    print(f"{'='*70}")
    print(f"  Rules processed: {len(remaining_rules)}")
    print(f"  Approved: {len(approved_rules)} ({len(approved_rules)/len(remaining_rules)*100:.1f}%)")
    print(f"  Skipped: {skipped_count} ({skipped_count/len(remaining_rules)*100:.1f}%)")
    print(f"  Errors: {error_count} ({error_count/len(remaining_rules)*100:.1f}%)")
    print(f"")
    print(f"  Vocabulary growth: {new_tags_added} new tags added")
    if approved_confidences:
        print(f"  Average confidence (approved): {avg_confidence:.2f}")
    print(f"")
    print(f"  Domain breakdown:")
    for domain, metrics in sorted(domain_metrics.items(), key=lambda x: x[1]['approved'], reverse=True):
        if metrics['total'] > 0:
            print(f"    {domain}: {metrics['total']} processed, {metrics['approved']} approved ({metrics['improvement_rate']*100:.0f}%)")

    return {
        'improvement_rate': improvement_rate,
        'any_domain_active': any_domain_active,
        'vocabulary_saturated': vocabulary_saturated,
        'quality_floor_reached': quality_floor_reached,
        'remaining_count': remaining_count,
        'approved_count': len(approved_rules),
        'avg_confidence': avg_confidence,
        'new_tags_added': new_tags_added,
        'domain_metrics': domain_metrics
    }


def should_stop_iteration(pass_results):
    """OPT-067: Master convergence decision."""
    if pass_results['remaining_count'] == 0:
        print("\n✓ All rules tagged")
        return True

    if pass_results['improvement_rate'] < 0.05 and not pass_results['any_domain_active']:
        print(f"\n✓ Convergence: Global {pass_results['improvement_rate']:.1%}, all domains converged")
        return True

    if pass_results['vocabulary_saturated']:
        print(f"\n✓ Vocabulary saturated: <3 new tags, {pass_results['improvement_rate']:.1%} improvement")
        return True

    if pass_results['quality_floor_reached']:
        print("\n⚠️  Quality floor reached (avg confidence <0.65)")
        return True

    return False


def get_database_statistics(db_path):
    """OPT-073: Get database statistics by tags_state."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tags_state, COUNT(*) as count
        FROM rules
        GROUP BY tags_state
    """)
    state_counts = dict(cursor.fetchall())

    cursor.execute("SELECT COUNT(*) FROM rules")
    total = cursor.fetchone()[0]

    conn.close()

    return {
        'total': total,
        'curated': state_counts.get('curated', 0),
        'refined': state_counts.get('refined', 0),
        'pending_review': state_counts.get('pending_review', 0),
        'needs_tags': state_counts.get('needs_tags', 0)
    }


def main():
    """Optimize rule tags through Claude reasoning with vocabulary intelligence and human oversight."""
    parser = argparse.ArgumentParser(description='Optimize rule tags with vocabulary intelligence')
    parser.add_argument('--tags-state', default='needs_tags', help='Filter by tags_state')
    parser.add_argument('--limit', type=int, help='Limit number of rules to process')
    parser.add_argument('--auto-approve', action='store_true', help='Auto-approve high-confidence tags')

    args = parser.parse_args()

    # Load configuration
    config = load_config()
    db_path = BASE_DIR / config['paths']['database']
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'

    # OPT-072, OPT-073: Check for empty database state
    stats = get_database_statistics(db_path)
    if stats['needs_tags'] == 0:
        print("No rules require tag optimization.")
        print("")
        print("Database state:")
        print(f"  Total rules: {stats['total']}")
        print(f"  Curated: {stats['curated']}")
        print(f"  Refined: {stats['refined']}")
        print(f"  Pending review: {stats['pending_review']}")
        print(f"  Needs tags: 0")
        print("")
        if stats['total'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules from chatlogs.")
        elif stats['curated'] + stats['refined'] > 0:
            print("All rules have been tagged. Use 'make tags-stats' to view tag distribution.")
        else:
            print("All pending rules require manual review. Run 'make tags-optimize' for interactive tagging.")
        return 0

    # Load vocabulary
    vocab = load_vocabulary(vocab_path)

    # OPT-056: Iterative mode when --auto-approve with NO --limit
    if args.auto_approve and not args.limit:
        # Multi-pass iterative optimization
        print("Tag Optimization - Multi-Pass Iterative Mode")
        print("="*70)

        # OPT-057: Load configuration
        tag_opt_config = config.get('tag_optimization', {})
        max_passes = tag_opt_config.get('convergence_max_passes', 10)
        confidence_threshold = tag_opt_config.get('confidence_threshold', 0.70)

        # OPT-057b: Calculate corpus size and cost limit
        conn = sqlite3.connect(str(db_path))
        corpus_size = len(conn.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'").fetchall())
        conn.close()

        # OPT-058: Cost limit
        cost_limit = max(500, int(corpus_size * 0.5))

        print(f"Corpus size: {corpus_size} rules")
        print(f"Cost limit: {cost_limit} LLM calls")
        print(f"Confidence threshold: {confidence_threshold}")
        print(f"Max passes: {max_passes}")

        pass_number = 0
        total_llm_calls = 0
        prev_avg_confidence = 0.0

        while pass_number < max_passes:
            # Get remaining rules
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            remaining_rules = conn.execute(
                "SELECT * FROM rules WHERE tags_state = 'needs_tags'"
            ).fetchall()
            conn.close()

            if len(remaining_rules) == 0:
                break

            if total_llm_calls >= cost_limit:
                print(f"\n✓ Cost limit reached ({total_llm_calls} >= {cost_limit} calls)")
                break

            # Run optimization pass
            pass_results = run_optimization_pass(
                remaining_rules, pass_number, vocab_path, config,
                confidence_threshold, prev_avg_confidence
            )

            # Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1
            total_llm_calls += len(remaining_rules)
            prev_avg_confidence = pass_results['avg_confidence']

        # OPT-070: Multi-pass summary report
        print(f"\n{'='*70}")
        print("Multi-Pass Optimization Complete")
        print(f"{'='*70}")
        print(f"Total passes: {pass_number + 1}")
        print(f"Total LLM calls: {total_llm_calls}")

        final_stats = get_database_statistics(db_path)
        print(f"\nFinal State:")
        print(f"  Total rules: {final_stats['total']}")
        print(f"  Tagged (curated + refined): {final_stats['curated'] + final_stats['refined']}")
        print(f"  Needs review: {final_stats['pending_review']}")
        print(f"  Needs tags: {final_stats['needs_tags']}")

        if final_stats['needs_tags'] > 0:
            print(f"\nNext Steps:")
            print(f"  {final_stats['needs_tags']} rules remain untagged.")
            print(f"  Run 'make tags-optimize' for interactive tagging.")

        return 0

    else:
        # Single-pass mode (interactive or auto-approve with limit)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        query = f"SELECT * FROM rules WHERE tags_state = ?"
        params = [args.tags_state]

        if args.limit:
            query += " LIMIT ?"
            params.append(args.limit)

        rules = conn.execute(query, params).fetchall()
        conn.close()

        if not rules:
            print(f"No rules found with tags_state = '{args.tags_state}'")
            return 0

        print(f"Tag Optimization - {'Auto-Approve' if args.auto_approve else 'Interactive'} Mode")
        print("="*70)
        print(f"Processing {len(rules)} rules with tags_state = '{args.tags_state}'")

        # Load template
        template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'
        with open(template_path) as f:
            template = f.read()

        vocab_formatted = format_vocabulary_for_prompt(vocab)

        processed = 0
        approved = 0
        skipped = 0

        for rule in rules:
            if args.auto_approve:
                result = process_rule_auto(rule, template, vocab, vocab_formatted, db_path, 0.70)
                if result['status'] == 'approved':
                    approved += 1
                else:
                    skipped += 1
                processed += 1
            else:
                result = process_rule_interactive(rule, template, vocab, vocab_formatted, db_path)
                if result['status'] == 'quit':
                    break
                elif result['status'] == 'approved':
                    approved += 1
                else:
                    skipped += 1
                processed += 1

        print(f"\n{'='*70}")
        print("Summary")
        print(f"{'='*70}")
        print(f"Processed: {processed}")
        print(f"Approved: {approved}")
        print(f"Skipped: {skipped}")

        return 0


if __name__ == '__main__':
    sys.exit(main())
