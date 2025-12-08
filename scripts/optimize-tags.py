#!/usr/bin/env python3
"""
Tag optimization with vocabulary-aware intelligence and HITL workflow

Implements constraints: OPT-001 through OPT-074
Generated from: build/modules/runtime-script-tag-optimization.yaml
Version: v1.5.11
"""

import sys
import json
import sqlite3
import subprocess
import argparse
import re
from pathlib import Path
from datetime import datetime, UTC
import fcntl
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml

# INV-021: Absolute paths only - read from config
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
CONFIG_PATH = BASE_DIR / "config" / "deployment.yaml"

# Load config to get project root and context engine home
with open(CONFIG_PATH) as f:
    _config = yaml.safe_load(f)
    PROJECT_ROOT = Path(_config['paths']['project_root'])
    BASE_DIR = Path(_config['paths']['context_engine_home'])


def load_config():
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def load_vocabulary(vocab_path):
    """OPT-019: Load vocabulary from tag-vocabulary.yaml (OPT-035, OPT-035a)."""
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)

        # OPT-019a: Verify required structure
        if not isinstance(vocab.get('tier_1_domains'), dict):
            raise ValueError("Missing or invalid tier_1_domains")
        if not isinstance(vocab.get('tier_2_tags'), dict):
            raise ValueError("Missing or invalid tier_2_tags")

        return vocab
    except Exception as e:
        # OPT-035a: Include path and reason
        print(f"Error: Failed to load vocabulary from {vocab_path}: {e}", file=sys.stderr)
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


def format_tier_2_tags(vocab):
    """OPT-034d: Format tier-2 tags with first 10 and ellipsis notation."""
    tier_2_tags_lines = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if len(tags) <= 10:
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
        else:
            remaining = len(tags) - 10
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
    return '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (none defined)'


def format_vocabulary_mappings(vocab):
    """OPT-034e: Format vocabulary mappings with first 5 examples."""
    mappings = vocab.get('vocabulary_mappings', {})
    if mappings and len(mappings) > 0:
        mapping_items = list(mappings.items())[:5]
        mapping_lines = [f'  "{word}" → {canonical}' for word, canonical in mapping_items]
        if len(mappings) > 5:
            mapping_lines.append(f"  ... (and {len(mappings) - 5} more)")
        return '\n'.join(mapping_lines)
    else:
        return '  (none defined)'


def format_synonyms(vocab):
    """OPT-034e variant: Format synonyms."""
    synonyms_content = vocab.get('synonyms', {})
    if synonyms_content:
        synonym_lines = [f"  {canonical}: {', '.join(variants)}"
                         for canonical, variants in list(synonyms_content.items())[:5]]
        if len(synonyms_content) > 5:
            synonym_lines.append(f"  ... (and {len(synonyms_content) - 5} more)")
        return '\n'.join(synonym_lines)
    else:
        return '  (none defined)'


def format_forbidden_stopwords(vocab):
    """OPT-034f: Format forbidden stopwords with first 20 and count notation."""
    stopwords = vocab.get('stopwords', [])
    if len(stopwords) <= 20:
        return ', '.join(stopwords)
    else:
        return ', '.join(stopwords[:20]) + f", ... (and {len(stopwords) - 20} more)"


def validate_response(response_data, vocab):
    """OPT-029 through OPT-033b: Validate Claude response."""
    errors = []

    # OPT-030: Validate tag count is between 2 and 5
    tags = response_data.get('tags', [])
    if not isinstance(tags, list) or len(tags) < 2 or len(tags) > 5:
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Validate tags against forbidden stopwords
    forbidden_stopwords = vocab.get('stopwords', [])
    for tag in tags:
        if tag in forbidden_stopwords:
            errors.append(f"forbidden stopword: '{tag}'")

    # OPT-031: Validate domain exists in tier_1_domains
    domain = response_data.get('domain')
    if domain and domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"invalid domain: '{domain}'")

    # OPT-032: Validate confidence score
    confidence = response_data.get('confidence')
    if confidence is None:
        # OPT-032a: Default to 0.5 if missing
        response_data['confidence'] = 0.5
    elif not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        errors.append(f"invalid confidence: {confidence}")
        response_data['confidence'] = 0.5

    return errors


def calculate_coherence(proposed_tags, domain, vocab):
    """OPT-050: Calculate coherence as precision metric."""
    domain_tags = vocab.get('tier_2_tags', {}).get(domain, [])

    # OPT-052: Bootstrap exception - domains with < 5 existing tags bypass coherence
    if len(domain_tags) < 5:
        return 1.0

    # Precision = intersection / len(proposed_tags)
    intersection = sum(1 for tag in proposed_tags if tag in domain_tags)
    if len(proposed_tags) == 0:
        return 0.0

    precision = intersection / len(proposed_tags)
    return precision


def optimize_single_rule(rule, template, vocab, config):
    """Call Claude CLI to optimize tags for a single rule (OPT-036, OPT-037)."""

    # OPT-034c: Format tier-1 domains as comma-separated list
    tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

    # OPT-034d through OPT-034f: Format vocabulary components
    tier_2_tags = format_tier_2_tags(vocab)
    vocabulary_mappings = format_vocabulary_mappings(vocab)
    synonyms = format_synonyms(vocab)
    forbidden_stopwords = format_forbidden_stopwords(vocab)

    # Substitute all variables in template
    prompt = template.format(
        rule_id=rule['id'],
        rule_type=rule['type'],
        title=rule['title'],
        description=rule['description'] or '',
        domain=rule['domain'] or '(unspecified)',
        tags=', '.join(json.loads(rule['tags'] or '[]')) or '(none)',
        tier_1_domains=tier_1_domains,
        tier_2_tags=tier_2_tags,
        vocabulary_mappings=vocabulary_mappings,
        synonyms=synonyms,
        forbidden_stopwords=forbidden_stopwords,
        session_context=''  # OPT-038: Not required
    )

    # OPT-036: Invoke Claude CLI with prompt as argument (not stdin)
    try:
        result = subprocess.run(
            ['claude', '--print', prompt],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL
        )

        if result.returncode != 0:
            # OPT-036: CLI failure
            return {
                'status': 'error',
                'error': f"Claude CLI failed: {result.stderr[:200]}"
            }

    except subprocess.TimeoutExpired:
        # OPT-036: Timeout
        return {
            'status': 'error',
            'error': 'Claude CLI timeout (30s)'
        }
    except FileNotFoundError:
        # Claude CLI not installed
        return {
            'status': 'error',
            'error': 'claude command not found'
        }

    # OPT-037b: Extract JSON from markdown code blocks
    raw_response = result.stdout.strip()
    json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', raw_response, re.DOTALL)
    if json_match:
        json_str = json_match.group(1).strip()
    else:
        json_str = raw_response

    # OPT-037: Parse JSON response
    try:
        response_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        # OPT-037: Parse failure
        return {
            'status': 'error',
            'error': f"JSON parse failed: {e}",
            'parse_failure': raw_response[:500]
        }

    # Validate response (OPT-029 through OPT-033b)
    validation_errors = validate_response(response_data, vocab)
    if validation_errors:
        # OPT-033b: Return with formatted error string
        return {
            'status': 'validation_failed',
            'error': '; '.join(validation_errors)
        }

    # Extract response fields
    suggested_tags = response_data.get('tags', [])
    suggested_domain = response_data.get('domain', rule['domain'] or 'general')
    confidence = response_data.get('confidence', 0.5)
    reasoning = response_data.get('reasoning', '')

    # OPT-050: Calculate coherence
    coherence = calculate_coherence(suggested_tags, suggested_domain, vocab)

    return {
        'status': 'success',
        'tags': suggested_tags,
        'domain': suggested_domain,
        'confidence': confidence,
        'reasoning': reasoning,
        'coherence': coherence
    }


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """OPT-039 through OPT-041a: Update vocabulary with approved tags."""

    # OPT-041a: File locking for thread-safe vocabulary updates
    with open(vocab_path, 'r+') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

        vocab = yaml.safe_load(f)
        if vocab is None:  # Handle corruption
            print(f"  ⚠ Warning: Vocabulary file corrupted, skipping update", file=sys.stderr)
            return

        # OPT-039a: Validate domain exists in tier_1_domains
        if rule_domain not in vocab.get('tier_1_domains', {}):
            # OPT-039c: Log warning
            log_path = BASE_DIR / 'data' / 'tag_optimization_warnings.log'
            log_path.parent.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
            with open(log_path, 'a') as log:
                log.write(f"{timestamp}\t{rule_id}\tinvalid_domain\t{rule_domain}\tskipped_vocabulary_update\n")

            print(f"  ⚠ Warning: Invalid domain '{rule_domain}' for {rule_id}, skipping vocabulary update")
            return

        # OPT-039b: Ensure tier_2_tags entry exists for valid domain
        if 'tier_2_tags' not in vocab:
            vocab['tier_2_tags'] = {}
        if rule_domain not in vocab['tier_2_tags']:
            vocab['tier_2_tags'][rule_domain] = []

        # OPT-039, OPT-040: Append new tags if not already present
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

        # Lock automatically released on context exit


def process_rule(rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold):
    """Process a single rule (worker thread function for OPT-044c)."""

    # OPT-044c: Create thread-local database connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Optimize tags via Claude
    result = optimize_single_rule(rule, template, vocab, None)

    # Prepare result dict
    result['rule_id'] = rule['id']

    if result['status'] != 'success':
        # OPT-036a, OPT-037a: Store error metadata
        error_metadata = json.loads(rule['metadata'] or '{}')
        if result['status'] == 'error':
            error_metadata['optimization_error'] = result['error']
        elif result['status'] == 'validation_failed':
            error_metadata['validation_failure'] = result['error']
            # OPT-033: Transition to pending_review
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(error_metadata), rule['id'])
            )
            conn.commit()

        conn.close()
        return result

    # OPT-011: Auto-approve logic with uniform 0.70 threshold
    confidence = result['confidence']
    coherence = result['coherence']

    if auto_approve:
        if confidence >= confidence_threshold and coherence >= 0.3:
            decision = 'approve'
        else:
            decision = 'skip'
    else:
        # Interactive mode - caller handles prompting
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
        metadata['optimization_reasoning'] = result['reasoning']
        metadata['tag_confidence'] = confidence
        metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

        # OPT-003: Update rule in database
        conn.execute(
            """UPDATE rules
               SET tags = ?, domain = ?, tags_state = ?, metadata = ?,
                   curated_at = ?, curated_by = ?
               WHERE id = ?""",
            (
                json.dumps(result['tags']),
                result['domain'],
                tags_state,
                json.dumps(metadata),
                datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                'Claude Sonnet 4.5',
                rule['id']
            )
        )
        conn.commit()

        # OPT-039: Update vocabulary
        update_vocabulary(rule['id'], result['domain'], result['tags'], vocab_path)

        result['status'] = 'approved'
    else:
        result['status'] = 'skipped'

    conn.close()
    return result


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, template, vocab,
                          db_path, auto_approve, confidence_threshold, max_workers):
    """OPT-057a: Execute single optimization pass, returns 9-key metrics dict."""

    print(f"\n{'='*70}")
    print(f"Pass {pass_number + 1}")
    print(f"{'='*70}")
    print(f"Processing {len(remaining_rules)} rules...")

    # OPT-062: Track vocabulary before pass
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # OPT-044a: Parallel processing with ThreadPoolExecutor
    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_rule, rule, template, vocab, vocab_path, db_path,
                auto_approve, confidence_threshold
            ): rule
            for rule in remaining_rules
        }

        for future in as_completed(futures):
            completed += 1
            rule = futures[future]

            try:
                result = future.result()

                # Track results
                if result['status'] == 'approved':
                    approved_rules.append(result)
                    approved_confidences.append(result['confidence'])
                elif result['status'] == 'error' or result['status'] == 'validation_failed':
                    error_count += 1
                elif result['status'] == 'skipped':
                    skipped_count += 1

                # OPT-044d: Print verbose progress
                if auto_approve:
                    status_icon = {'approved': '✓', 'skipped': '⊘', 'error': '✗'}.get(result['status'], '?')
                    confidence = result.get('confidence', 0.0)
                    coherence = result.get('coherence', 0.0)

                    print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                    print(f"    Title: {rule['title']}")
                    print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")

                    if result['status'] == 'approved':
                        print(f"    Decision: approved")
                    elif result['status'] == 'skipped':
                        if confidence < confidence_threshold:
                            print(f"    Decision: skipped (confidence < {confidence_threshold:.2f})")
                        elif coherence < 0.3:
                            print(f"    Decision: skipped (coherence < 0.3)")
                        else:
                            print(f"    Decision: skipped")
                    elif result['status'] == 'error' or result['status'] == 'validation_failed':
                        print(f"    Decision: error")
                        if result.get('error'):
                            print(f"    Error: {result['error']}")

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
                error_count += 1
                print(f"  ✗ Exception processing {rule['id']}: {e}", file=sys.stderr)

    # OPT-062: Track vocabulary after pass
    tags_after = set(load_all_tier2_tags_from_vocabulary(vocab_path))
    new_tags_added = len(tags_after - tags_before)

    # OPT-059: Calculate improvement rate
    improvement_rate = len(approved_rules) / len(remaining_rules) if remaining_rules else 0.0

    # OPT-064: Calculate average confidence
    avg_confidence = sum(approved_confidences) / len(approved_confidences) if approved_confidences else 0.0

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

    # OPT-061: Domain-level convergence check
    any_domain_active = any(metrics['improvement_rate'] > 0.10 for metrics in domain_metrics.values())

    # OPT-063: Vocabulary saturation detection
    vocabulary_saturated = (new_tags_added < 3 and improvement_rate < 0.10)

    # OPT-065: Quality degradation detection (handled by caller comparing passes)
    quality_floor_reached = (avg_confidence < 0.65) if approved_confidences else False

    # Print pass summary
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

    # OPT-057a: Return 9-key metrics dict
    return {
        'improvement_rate': improvement_rate,
        'any_domain_active': any_domain_active,
        'vocabulary_saturated': vocabulary_saturated,
        'quality_floor_reached': quality_floor_reached,
        'remaining_count': len(remaining_rules) - len(approved_rules),
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
        print("\n⚠️ Quality floor reached (avg confidence <0.65)")
        return True

    return False


def run_iterative_optimization(db_path, vocab_path, config, auto_approve):
    """OPT-056, OPT-057: Multi-pass iterative optimization."""

    # Load configuration
    tag_opt_config = config.get('tag_optimization', {})
    max_passes = tag_opt_config.get('convergence_max_passes', 10)
    max_workers = tag_opt_config.get('max_workers', 3)
    confidence_threshold = 0.70  # OPT-011, OPT-045: Uniform threshold

    # OPT-057b: Calculate corpus size
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    corpus_size = len(conn.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'").fetchall())
    conn.close()

    # OPT-058: Calculate cost limit
    cost_limit = max(500, int(corpus_size * 0.5))

    print(f"\nIterative Tag Optimization")
    print(f"{'='*70}")
    print(f"Corpus size: {corpus_size} rules")
    print(f"Cost limit: {cost_limit} API calls")
    print(f"Max passes: {max_passes}")
    print(f"Confidence threshold: {confidence_threshold:.2f}")
    print(f"Coherence threshold: 0.30")
    print(f"Max workers: {max_workers}")

    # Load template and vocabulary
    vocab = load_vocabulary(vocab_path)
    template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'
    with open(template_path) as f:
        template = f.read()

    # Multi-pass loop
    pass_number = 0
    total_llm_calls = 0
    prev_avg_confidence = None

    while pass_number < max_passes:
        # Query remaining rules
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        remaining_rules = conn.execute(
            "SELECT * FROM rules WHERE tags_state = 'needs_tags'"
        ).fetchall()
        conn.close()

        if len(remaining_rules) == 0:
            print("\n✓ No rules require optimization")
            break

        # OPT-058: Check cost limit (between-pass)
        if total_llm_calls >= cost_limit:
            print(f"\n⚠️ Cost limit reached ({total_llm_calls}/{cost_limit} API calls)")
            break

        # Reload vocabulary each pass (OPT-046: vocabulary grows between passes)
        vocab = load_vocabulary(vocab_path)

        # Run optimization pass
        pass_results = run_optimization_pass(
            remaining_rules, pass_number, vocab_path, config, template, vocab,
            db_path, auto_approve, confidence_threshold, max_workers
        )

        total_llm_calls += len(remaining_rules)

        # OPT-065: Quality degradation detection
        if pass_number > 0 and prev_avg_confidence is not None:
            confidence_drop = prev_avg_confidence - pass_results['avg_confidence']
            if confidence_drop > 0.15:
                print(f"\n⚠️ Warning: Confidence dropped {confidence_drop:.2f} from previous pass")

        prev_avg_confidence = pass_results['avg_confidence']

        # OPT-067: Check convergence
        if should_stop_iteration(pass_results):
            break

        pass_number += 1

    # Final summary
    print(f"\n{'='*70}")
    print(f"Iterative Optimization Complete")
    print(f"{'='*70}")
    print(f"Total passes: {pass_number + 1}")
    print(f"Total API calls: {total_llm_calls}")

    # Query final state
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    final_stats = {
        'total': conn.execute("SELECT COUNT(*) as c FROM rules").fetchone()['c'],
        'curated': conn.execute("SELECT COUNT(*) as c FROM rules WHERE tags_state = 'curated'").fetchone()['c'],
        'refined': conn.execute("SELECT COUNT(*) as c FROM rules WHERE tags_state = 'refined'").fetchone()['c'],
        'pending_review': conn.execute("SELECT COUNT(*) as c FROM rules WHERE tags_state = 'pending_review'").fetchone()['c'],
        'needs_tags': conn.execute("SELECT COUNT(*) as c FROM rules WHERE tags_state = 'needs_tags'").fetchone()['c']
    }
    conn.close()

    print(f"\nFinal State:")
    print(f"  Total rules: {final_stats['total']}")
    print(f"  Curated: {final_stats['curated']}")
    print(f"  Refined: {final_stats['refined']}")
    print(f"  Pending review: {final_stats['pending_review']}")
    print(f"  Needs tags: {final_stats['needs_tags']}")

    if final_stats['needs_tags'] > 0:
        print(f"\nNext Steps:")
        print(f"  • {final_stats['needs_tags']} rules require manual review")
        print(f"  • Run 'optimize-tags.py' (without --auto-approve) for interactive tagging")


def get_database_statistics(db_path):
    """OPT-073: Get database statistics for empty-state reporting."""
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


def main():
    """OPT-001: Tag optimization optimizes rule tags through Claude reasoning with human oversight."""

    parser = argparse.ArgumentParser(
        description='Optimize rule tags with vocabulary-aware intelligence'
    )
    parser.add_argument(
        '--auto-approve',
        action='store_true',
        help='Auto-approve tags meeting confidence and coherence thresholds'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rules to process'
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config()

    # OPT-055a: Database path from config['structure']['database_path']
    db_path = BASE_DIR / config['structure']['database_path']

    # OPT-019b: Vocabulary path relative to BASE_DIR
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')

    # OPT-072: Check if any rules need optimization
    conn = sqlite3.connect(str(db_path))
    needs_tags_count = conn.execute(
        "SELECT COUNT(*) as c FROM rules WHERE tags_state = 'needs_tags'"
    ).fetchone()['c']
    conn.close()

    if needs_tags_count == 0:
        # OPT-072, OPT-073: Empty-state reporting
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

        return 0

    # OPT-056: Iterative mode when --auto-approve without --limit
    if args.auto_approve and args.limit is None:
        run_iterative_optimization(db_path, vocab_path, config, args.auto_approve)
        return 0

    # Single-batch mode (interactive or limited auto-approve)
    print("Single-batch tag optimization not yet implemented.")
    print("Use --auto-approve (without --limit) for iterative multi-pass optimization.")
    return 1


if __name__ == '__main__':
    sys.exit(main())
