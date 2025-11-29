#!/usr/bin/env python3
"""
Tag optimization with vocabulary-aware intelligence and HITL workflow

Implements constraints: OPT-001 through OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.10.yaml
"""

import sys
import json
import sqlite3
from pathlib import Path

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml
import argparse
import subprocess
import re
from datetime import datetime, timezone
import fcntl  # OPT-041a: File locking for thread-safe vocabulary updates
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

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
# RUNTIME-SCRIPT-TAG-OPTIMIZATION MODULE IMPLEMENTATION
# ============================================================================

# OPT-060a: Extract tier-1 domains from vocabulary
def get_tier_1_domains(vocab_path):
    """Extract tier-1 domain names from vocabulary file."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    tier_1_domains = list(vocab.get('tier_1_domains', {}).keys())
    return tier_1_domains


# OPT-062a: Load all tier-2 tags from vocabulary
def load_all_tier2_tags_from_vocabulary():
    """Load all tier-2 tags from vocabulary file across all domains."""
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    all_tags = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        all_tags.extend(tags)
    return all_tags


# OPT-073: Get database statistics
def get_database_statistics(db_path):
    """Get database statistics including total and per-state counts."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get counts by tags_state
    cursor.execute("""
        SELECT tags_state, COUNT(*) as count
        FROM rules
        GROUP BY tags_state
    """)
    state_counts = dict(cursor.fetchall())

    # Get total
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


# OPT-039 through OPT-041a: Update vocabulary with approved tags
def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """Update vocabulary with approved tags using file locking."""
    # OPT-041a: Exclusive file locking
    with open(vocab_path, 'r+') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

        vocab = yaml.safe_load(f)
        if vocab is None:
            return

        # OPT-039a: Validate domain exists in tier_1_domains
        if rule_domain not in vocab.get('tier_1_domains', {}):
            # OPT-039c: Log warning
            log_path = BASE_DIR / 'data' / 'tag_optimization_warnings.log'
            timestamp = datetime.now(timezone.utc).isoformat()
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, 'a') as log:
                log.write(f"{timestamp}\t{rule_id}\tinvalid_domain\t{rule_domain}\tskipped_vocabulary_update\n")
            return

        # OPT-039b: Create tier_2_tags[domain] entry if missing
        if 'tier_2_tags' not in vocab:
            vocab['tier_2_tags'] = {}
        if rule_domain not in vocab['tier_2_tags']:
            vocab['tier_2_tags'][rule_domain] = []

        # OPT-039, OPT-040: Append new tags only if not already present
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


# OPT-029 through OPT-033b: Validate Claude response
def validate_response(response_data, vocab, rule_domain):
    """Validate Claude response against vocabulary constraints."""
    errors = []

    # OPT-030: Validate tag count
    tags = response_data.get('tags', [])
    if not (2 <= len(tags) <= 5):
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Validate no forbidden stopwords
    forbidden_stopwords = vocab.get('forbidden', {}).get('stopwords', [])
    for tag in tags:
        if tag in forbidden_stopwords:
            errors.append(f"forbidden stopword '{tag}' in tags")

    # OPT-031: Validate domain exists in tier_1_domains
    domain = response_data.get('domain', rule_domain)
    if domain and domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"invalid domain '{domain}'")

    # OPT-032: Validate confidence score
    confidence = response_data.get('confidence')
    if confidence is None or not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        # OPT-032a: Default to 0.5 if invalid
        response_data['confidence'] = 0.5

    return errors


# OPT-050 through OPT-054: Calculate tag coherence
def calculate_coherence(proposed_tags, domain_vocab_tags):
    """Calculate coherence as precision: intersection / proposed_tags."""
    # OPT-050: Precision metric
    intersection = sum(1 for tag in proposed_tags if tag in domain_vocab_tags)
    if len(proposed_tags) == 0:
        return 0.0
    precision = intersection / len(proposed_tags)
    return precision


# OPT-036, OPT-037: Call Claude CLI and parse response
def call_claude_cli(prompt, rule_id, db_path):
    """Call Claude CLI with prompt and parse JSON response."""
    try:
        # OPT-036: Call Claude CLI with prompt as argument (not stdin)
        result = subprocess.run(
            ['claude', '--print', prompt],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL
        )

        if result.returncode != 0:
            # OPT-036, OPT-036a: CLI failure
            return {
                'status': 'error',
                'error': f"Claude CLI failed: {result.stderr[:200]}"
            }

    except subprocess.TimeoutExpired:
        return {
            'status': 'error',
            'error': "Claude CLI timeout"
        }
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

    # OPT-037: Parse JSON
    try:
        response_data = json.loads(json_str)
        return {
            'status': 'success',
            'data': response_data
        }
    except json.JSONDecodeError as e:
        # OPT-037, OPT-037a: Parse failure
        return {
            'status': 'parse_error',
            'error': str(e),
            'raw_response': raw_response[:500]
        }


# OPT-044c: Process single rule (runs in worker thread)
def process_rule(rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold):
    """Process a single rule optimization (thread-safe)."""
    # OPT-044c: Create thread-local database connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rule_id = rule['id']
    rule_domain = rule['domain'] or 'general'

    # Format vocabulary components per OPT-034c through OPT-034f
    tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

    tier_2_tags_lines = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if len(tags) <= 10:
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
        else:
            remaining = len(tags) - 10
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
    tier_2_tags = '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (none defined)'

    mappings = vocab.get('vocabulary_mappings', {})
    if mappings and len(mappings) > 0:
        mapping_items = list(mappings.items())[:5]
        mapping_lines = [f'  "{word}" → {canonical}' for word, canonical in mapping_items]
        if len(mappings) > 5:
            mapping_lines.append(f"  ... (and {len(mappings) - 5} more)")
        vocabulary_mappings = '\n'.join(mapping_lines)
    else:
        vocabulary_mappings = '  (none defined)'

    synonyms_content = vocab.get('synonyms', {})
    if synonyms_content:
        synonym_lines = [f"  {canonical}: {', '.join(variants)}" for canonical, variants in list(synonyms_content.items())[:5]]
        if len(synonyms_content) > 5:
            synonym_lines.append(f"  ... (and {len(synonyms_content) - 5} more)")
        synonyms = '\n'.join(synonym_lines)
    else:
        synonyms = '  (none defined)'

    stopwords = vocab.get('forbidden', {}).get('stopwords', [])
    if len(stopwords) <= 20:
        forbidden_stopwords = ', '.join(stopwords)
    else:
        forbidden_stopwords = ', '.join(stopwords[:20]) + f", ... (and {len(stopwords) - 20} more)"

    # Format prompt
    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'] or '',
        domain=rule_domain,
        tags=', '.join(json.loads(rule['tags'] or '[]')) or '(none)',
        tier_1_domains=tier_1_domains,
        tier_2_tags=tier_2_tags,
        vocabulary_mappings=vocabulary_mappings,
        synonyms=synonyms,
        forbidden_stopwords=forbidden_stopwords,
        session_context=''
    )

    # Call Claude CLI
    result = call_claude_cli(prompt, rule_id, db_path)

    if result['status'] == 'error':
        # OPT-036a: Store error in metadata
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['optimization_error'] = result['error']
        conn.execute(
            "UPDATE rules SET metadata = ? WHERE id = ?",
            (json.dumps(metadata), rule_id)
        )
        conn.commit()
        conn.close()
        return {
            'status': 'error',
            'rule_id': rule_id,
            'error': result['error']
        }

    if result['status'] == 'parse_error':
        # OPT-037a: Store parse failure in metadata
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['parse_failure'] = result['raw_response']
        conn.execute(
            "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
            ('pending_review', json.dumps(metadata), rule_id)
        )
        conn.commit()
        conn.close()
        return {
            'status': 'error',
            'rule_id': rule_id,
            'error': f"JSON parse error: {result['error']}"
        }

    response_data = result['data']

    # Validate response
    validation_errors = validate_response(response_data, vocab, rule_domain)
    if validation_errors:
        # OPT-033, OPT-033a, OPT-033b: Validation failure
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['validation_failure'] = '; '.join(validation_errors)
        conn.execute(
            "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
            ('pending_review', json.dumps(metadata), rule_id)
        )
        conn.commit()
        conn.close()
        return {
            'status': 'validation_failed',
            'rule_id': rule_id,
            'error': '; '.join(validation_errors)
        }

    # Extract fields
    suggested_tags = response_data.get('tags', [])
    confidence = response_data.get('confidence', 0.5)
    reasoning = response_data.get('reasoning', '')

    # OPT-050, OPT-051, OPT-052: Calculate coherence
    domain_vocab_tags = vocab.get('tier_2_tags', {}).get(rule_domain, [])
    coherence = calculate_coherence(suggested_tags, domain_vocab_tags)

    # OPT-011: Auto-approve logic with uniform 0.70 threshold
    if auto_approve:
        # OPT-052: Bootstrap exception - domains with < 5 tags bypass coherence check
        if len(domain_vocab_tags) < 5:
            coherence_pass = True
        else:
            coherence_pass = coherence >= 0.3

        if confidence >= confidence_threshold and coherence_pass:
            decision = 'approve'
        else:
            decision = 'skip'
    else:
        # Interactive mode - return for user decision
        decision = 'interactive'

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
        metadata['optimized_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

        # OPT-003: Update rule
        conn.execute(
            """UPDATE rules
               SET tags = ?, tags_state = ?, metadata = ?,
                   curated_at = ?, curated_by = ?
               WHERE id = ?""",
            (json.dumps(suggested_tags), tags_state, json.dumps(metadata),
             datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
             'Claude Sonnet 4.5', rule_id)
        )
        conn.commit()

        # OPT-039 through OPT-041a: Update vocabulary
        update_vocabulary(rule_id, rule_domain, suggested_tags, vocab_path)

    conn.close()

    return {
        'status': 'approved' if decision == 'approve' else 'skipped',
        'rule_id': rule_id,
        'tags': suggested_tags,
        'confidence': confidence,
        'coherence': coherence,
        'reasoning': reasoning,
        'domain': rule_domain
    }


# OPT-067: Master convergence decision
def should_stop_iteration(pass_results):
    """Determine if iteration should stop based on convergence signals."""
    if pass_results['remaining_count'] == 0:
        print("✓ All rules tagged")
        return True
    if pass_results['improvement_rate'] < 0.05 and not pass_results['any_domain_active']:
        print(f"✓ Convergence: Global {pass_results['improvement_rate']:.1%}, all domains converged")
        return True
    if pass_results['vocabulary_saturated']:
        print(f"✓ Vocabulary saturated: <3 new tags, {pass_results['improvement_rate']:.1%} improvement")
        return True
    if pass_results['quality_floor_reached']:
        print("⚠ Quality floor reached (avg confidence <0.65)")
        return True
    return False


# OPT-057a: Run single optimization pass
def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, db_path, auto_approve, confidence_threshold, prev_avg_confidence):
    """Execute single optimization pass with parallel processing."""
    # Load vocabulary
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

    # Load template
    template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'
    with open(template_path) as f:
        template = f.read()

    # OPT-062: Track vocabulary before pass
    tags_before = set(load_all_tier2_tags_from_vocabulary())

    # OPT-044b: max_workers defaults to 3
    max_workers = 3

    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    # OPT-044a, OPT-044c: Parallel processing with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_rule, rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold): rule
            for rule in remaining_rules
        }

        completed = 0
        for future in as_completed(futures):
            rule = futures[future]
            completed += 1

            try:
                result = future.result()

                # OPT-044d: Verbose progress output
                if auto_approve:
                    status_icon = {'approved': '✓', 'skipped': '⊘', 'error': '✗'}.get(result['status'], '?')
                    confidence = result.get('confidence', 0.0)
                    coherence = result.get('coherence', 0.0)

                    print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                    print(f"    Title: {rule['title']}")
                    print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")

                    if result['status'] == 'approved':
                        print(f"    Decision: approved")
                        approved_rules.append(result)
                        approved_confidences.append(confidence)
                    elif result['status'] == 'skipped':
                        if confidence < confidence_threshold:
                            print(f"    Decision: skipped (confidence < {confidence_threshold})")
                        elif coherence < 0.3:
                            print(f"    Decision: skipped (coherence < 0.3)")
                        else:
                            print(f"    Decision: skipped")
                        skipped_count += 1
                    elif result['status'] == 'error':
                        print(f"    Decision: error")
                        if result.get('error'):
                            print(f"    Error: {result['error']}")
                        error_count += 1

                    if result.get('reasoning'):
                        reasoning_lines = result['reasoning'].split('\n')
                        print(f"    Reasoning: {reasoning_lines[0]}")
                        for line in reasoning_lines[1:]:
                            if line.strip():
                                print(f"               {line}")

                    if result.get('tags'):
                        if result['status'] == 'approved':
                            print(f"    Approved Tags: {', '.join(result['tags'])}")
                        else:
                            print(f"    Suggested Tags: {', '.join(result['tags'])}")

            except Exception as e:
                print(f"✗ Error processing {rule['id']}: {e}", file=sys.stderr)
                error_count += 1

    # OPT-062: Track vocabulary after pass
    tags_after = set(load_all_tier2_tags_from_vocabulary())
    new_tags_count = len(tags_after - tags_before)

    # OPT-059: Calculate improvement rate
    improvement_rate = len(approved_rules) / len(remaining_rules) if len(remaining_rules) > 0 else 0.0

    # OPT-064: Average confidence
    avg_confidence = sum(approved_confidences) / len(approved_confidences) if approved_confidences else 0.0

    # OPT-065: Quality degradation detection
    quality_floor_reached = False
    if pass_number > 0 and prev_avg_confidence is not None:
        confidence_drop = prev_avg_confidence - avg_confidence
        if confidence_drop > 0.15:
            print(f"⚠ Warning: Confidence dropped by {confidence_drop:.2f}")
        if avg_confidence < 0.65:
            print(f"⚠ Warning: Average confidence {avg_confidence:.2f} below quality floor 0.65")
            quality_floor_reached = True

    # OPT-060, OPT-060a: Domain-specific metrics
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

    # OPT-063: Vocabulary saturation
    vocabulary_saturated = (new_tags_count < 3 and improvement_rate < 0.10)

    # OPT-049: Pass summary
    print(f"\n{'='*70}")
    print(f"Pass {pass_number + 1} Summary")
    print(f"{'='*70}")
    print(f"  Rules processed: {len(remaining_rules)}")
    print(f"  Approved: {len(approved_rules)} ({len(approved_rules)/len(remaining_rules)*100:.1f}%)")
    print(f"  Skipped: {skipped_count} ({skipped_count/len(remaining_rules)*100:.1f}%)")
    print(f"  Errors: {error_count} ({error_count/len(remaining_rules)*100:.1f}%)")
    print(f"")
    print(f"  Vocabulary growth: {new_tags_count} new tags added")
    if approved_confidences:
        print(f"  Average confidence (approved): {avg_confidence:.2f}")
    print(f"")
    print(f"  Domain breakdown:")
    for domain, metrics in sorted(domain_metrics.items(), key=lambda x: x[1]['approved'], reverse=True):
        if metrics['total'] > 0:
            print(f"    {domain}: {metrics['total']} processed, {metrics['approved']} approved ({metrics['improvement_rate']*100:.0f}%)")

    # OPT-057a: Return 9-key dict
    return {
        'improvement_rate': improvement_rate,
        'any_domain_active': any_domain_active,
        'vocabulary_saturated': vocabulary_saturated,
        'quality_floor_reached': quality_floor_reached,
        'remaining_count': len(remaining_rules) - len(approved_rules),
        'approved_count': len(approved_rules),
        'avg_confidence': avg_confidence,
        'new_tags_added': new_tags_count,
        'domain_metrics': domain_metrics
    }


def main():
    """Optimize rule tags through Claude reasoning with vocabulary intelligence and human oversight."""
    # Parse arguments
    parser = argparse.ArgumentParser(description='Optimize rule tags with vocabulary awareness')
    parser.add_argument('--auto-approve', action='store_true', help='Auto-approve tags meeting confidence threshold')
    parser.add_argument('--limit', type=int, help='Limit number of rules to process')
    parser.add_argument('--tags-state', default='needs_tags', help='Filter by tags_state (default: needs_tags)')
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # OPT-019, OPT-019b: Load vocabulary
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
    except Exception as e:
        # OPT-035, OPT-035a: Vocabulary load failure
        print(f"Error loading vocabulary from {vocab_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Get database path
    db_path = BASE_DIR / config['structure']['database_path']

    # OPT-072, OPT-073: Check if database has rules needing optimization
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags_count = cursor.fetchone()[0]

    if needs_tags_count == 0:
        # OPT-072: Report database state
        stats = get_database_statistics(db_path)
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

        conn.close()
        return 0

    # OPT-056: Check for iterative mode (--auto-approve with no --limit)
    iterative_mode = args.auto_approve and args.limit is None

    if iterative_mode:
        # OPT-057, OPT-057b, OPT-058: Multi-pass iterative optimization
        print("Context Engine - Tag Optimization (Iterative Mode)")
        print("="*70)

        # Load build config
        build_config_path = BASE_DIR.parent / 'build' / 'config' / 'build-constants.yaml'
        try:
            with open(build_config_path) as f:
                build_config = yaml.safe_load(f)
        except:
            build_config = {}

        tag_opt_config = build_config.get('tag_optimization', {})
        max_passes = tag_opt_config.get('convergence_max_passes', 10)

        # OPT-057b: Calculate corpus size
        cursor.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'")
        corpus_size = len(cursor.fetchall())

        # OPT-058: Calculate cost limit
        cost_limit = max(500, int(corpus_size * 0.5))

        print(f"Corpus size: {corpus_size} rules")
        print(f"Cost limit: {cost_limit} LLM calls")
        print(f"Max passes: {max_passes}")
        print("")

        pass_number = 0
        total_llm_calls = 0
        prev_avg_confidence = None

        # OPT-045: Uniform 0.70 threshold
        confidence_threshold = 0.70

        while pass_number < max_passes:
            # Query remaining rules
            cursor.execute("SELECT * FROM rules WHERE tags_state = 'needs_tags'")
            remaining_rules = cursor.fetchall()

            if len(remaining_rules) == 0:
                break

            if total_llm_calls >= cost_limit:
                print(f"Cost limit reached ({total_llm_calls} >= {cost_limit}), stopping iteration")
                break

            print(f"\n{'='*70}")
            print(f"Starting Pass {pass_number + 1}")
            print(f"{'='*70}")
            print(f"Remaining rules: {len(remaining_rules)}")
            print("")

            # OPT-057a: Run optimization pass
            pass_results = run_optimization_pass(
                remaining_rules, pass_number, vocab_path, config, db_path,
                True, confidence_threshold, prev_avg_confidence
            )

            # OPT-067: Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1
            total_llm_calls += len(remaining_rules)
            prev_avg_confidence = pass_results['avg_confidence']

        # OPT-070: Multi-pass summary
        print(f"\n{'='*70}")
        print("Multi-Pass Optimization Complete")
        print(f"{'='*70}")
        print(f"Total passes: {pass_number}")
        print(f"Total LLM calls: {total_llm_calls}")
        print("")

        final_stats = get_database_statistics(db_path)
        print("Final State:")
        print(f"  Total rules: {final_stats['total']}")
        print(f"  Tagged (curated + refined): {final_stats['curated'] + final_stats['refined']}")
        print(f"  Pending review: {final_stats['pending_review']}")
        print(f"  Needs tags: {final_stats['needs_tags']}")

        if final_stats['needs_tags'] > 0:
            print("")
            print("Next Steps:")
            print(f"  {final_stats['needs_tags']} rules still require tagging")
            print("  Run 'make tags-optimize' for interactive review")

    else:
        # Single-pass or interactive mode
        print("Context Engine - Tag Optimization")
        print("="*70)
        print(f"Processing rules with tags_state: {args.tags_state}")
        if args.limit:
            print(f"Limit: {args.limit} rules")
        print("")

        # Query rules
        query = f"SELECT * FROM rules WHERE tags_state = ?"
        params = [args.tags_state]

        if args.limit:
            query += " LIMIT ?"
            params.append(args.limit)

        cursor.execute(query, params)
        rules = cursor.fetchall()

        print(f"Found {len(rules)} rules to process")
        print("")

        if len(rules) == 0:
            print("No rules to process.")
            conn.close()
            return 0

        # Process with single pass
        confidence_threshold = 0.70
        pass_results = run_optimization_pass(
            rules, 0, vocab_path, config, db_path,
            args.auto_approve, confidence_threshold, None
        )

        print("")
        print(f"Optimization complete: {pass_results['approved_count']} approved, {pass_results['remaining_count']} remaining")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
