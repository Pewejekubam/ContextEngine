#!/usr/bin/env python3
"""
Tag optimization with vocabulary-aware intelligence and HITL workflow

Implements constraints: OPT-001 through OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.11.yaml
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
import fcntl
from datetime import datetime, UTC
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


def load_vocabulary(vocab_path):
    """Load tag vocabulary from YAML file (OPT-019, OPT-019a)."""
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab
    except Exception as e:
        # OPT-035, OPT-035a: Vocabulary load failure aborts
        print(f"Error loading vocabulary from {vocab_path}: {e}", file=sys.stderr)
        sys.exit(1)


def get_tier_1_domains(vocab_path):
    """Extract tier-1 domain names from vocabulary file (OPT-060a)."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    tier_1_domains = list(vocab.get('tier_1_domains', {}).keys())
    return tier_1_domains


def load_all_tier2_tags_from_vocabulary(vocab_path):
    """Load all tier-2 tags from vocabulary file across all domains (OPT-062a)."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    all_tags = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        all_tags.extend(tags)
    return all_tags


def validate_response(response_data, vocab):
    """Validate Claude's response (OPT-029 through OPT-033b)."""
    errors = []

    # OPT-030: Tag count validation (2-5)
    tags = response_data.get('tags', [])
    if not (2 <= len(tags) <= 5):
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Forbidden stopwords validation
    forbidden_stopwords = vocab.get('stopwords', [])
    for tag in tags:
        if tag in forbidden_stopwords:
            errors.append(f"tag '{tag}' is forbidden stopword")

    # OPT-031: Domain validation
    domain = response_data.get('domain')
    if domain and domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"domain '{domain}' not in tier_1_domains")

    # OPT-032: Confidence validation
    confidence = response_data.get('confidence')
    if confidence is not None:
        if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
            errors.append(f"confidence must be float 0.0-1.0, got {confidence}")

    if errors:
        # OPT-033b: Return dict with status and error string
        return {
            'status': 'validation_failed',
            'error': '; '.join(errors)
        }

    return None


def calculate_coherence(proposed_tags, domain, vocab):
    """Calculate coherence as precision metric (OPT-050, OPT-051, OPT-052)."""
    domain_tags = vocab.get('tier_2_tags', {}).get(domain, [])

    # OPT-052: Bootstrap exception - domains with < 5 tags bypass check
    if len(domain_tags) < 5:
        return 1.0  # Trust early approvals

    # OPT-050: Coherence = intersection / len(proposed_tags) (precision)
    intersection = sum(1 for tag in proposed_tags if tag in domain_tags)
    if len(proposed_tags) == 0:
        return 0.0

    precision = intersection / len(proposed_tags)
    return precision


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """Update vocabulary with approved tags using file locking (OPT-039 through OPT-041a)."""

    # OPT-041a: Exclusive file locking for thread-safe vocabulary updates
    try:
        with open(vocab_path, 'r+') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # Block until lock acquired

            vocab = yaml.safe_load(f)
            if vocab is None:  # Handle corruption
                print(f"  ⚠ Warning: Vocabulary file corrupted, skipping update")
                return

            # OPT-039a: Validate domain exists in tier_1_domains
            if rule_domain not in vocab.get('tier_1_domains', {}):
                # OPT-039c: Log warning
                log_path = BASE_DIR / 'data' / 'tag_optimization_warnings.log'
                timestamp = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(log_path, 'a') as log:
                    log.write(f"{timestamp}\t{rule_id}\tinvalid_domain\t{rule_domain}\tskipped_vocabulary_update\n")

                print(f"  ⚠ Warning: Invalid domain '{rule_domain}' for {rule_id}, skipping vocabulary update")
                return  # Skip vocabulary update for invalid domain

            # OPT-039b: Ensure tier_2_tags entry exists for valid domain
            if 'tier_2_tags' not in vocab:
                vocab['tier_2_tags'] = {}
            if rule_domain not in vocab['tier_2_tags']:
                vocab['tier_2_tags'][rule_domain] = []

            # OPT-039, OPT-040: Append new tags (skip if already present)
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
    except Exception as e:
        print(f"  ⚠ Warning: Failed to update vocabulary: {e}")


def optimize_single_rule(rule, template, vocab, vocab_path, db_path, auto_approve, config):
    """Optimize tags for a single rule (worker thread function)."""

    # OPT-044c: Create thread-local database connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    result = {
        'rule_id': rule['id'],
        'status': 'error',
        'tags': [],
        'confidence': 0.0,
        'coherence': 0.0,
        'reasoning': ''
    }

    try:
        # Format vocabulary components (OPT-034c through OPT-034f)
        tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

        tier_2_tags_lines = []
        for domain, tags in vocab.get('tier_2_tags', {}).items():
            if len(tags) <= 10:
                tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
            else:
                remaining = len(tags) - 10
                tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
        tier_2_tags = '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (empty - bootstrap mode)'

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

        stopwords = vocab.get('stopwords', [])
        if len(stopwords) <= 20:
            forbidden_stopwords = ', '.join(stopwords)
        else:
            forbidden_stopwords = ', '.join(stopwords[:20]) + f", ... (and {len(stopwords) - 20} more)"

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
            session_context=''  # OPT-038: Not required, can be empty
        )

        # OPT-036, OPT-037: Invoke Claude CLI
        try:
            # OPT-036, OPT-037: Prompt passed as argument, not stdin
            claude_result = subprocess.run(
                ['claude', '--print', prompt],
                capture_output=True,
                text=True,
                timeout=30,
                stdin=subprocess.DEVNULL
            )

            if claude_result.returncode != 0:
                # OPT-036: CLI failure - log and update metadata
                result['status'] = 'error'
                result['error'] = claude_result.stderr[:200]

                # OPT-036a: Store error in metadata
                error_metadata = json.loads(rule['metadata'] or '{}')
                error_metadata['optimization_error'] = claude_result.stderr[:200]
                conn.execute(
                    "UPDATE rules SET metadata = ? WHERE id = ?",
                    (json.dumps(error_metadata), rule['id'])
                )
                conn.commit()
                conn.close()
                return result

        except subprocess.TimeoutExpired:
            # OPT-036: Timeout is a CLI failure
            result['status'] = 'error'
            result['error'] = 'Claude CLI timeout'
            conn.close()
            return result
        except FileNotFoundError:
            # Claude CLI not installed
            result['status'] = 'error'
            result['error'] = 'claude command not found'
            conn.close()
            return result

        # OPT-037b: Extract JSON from markdown code blocks if present
        raw_response = claude_result.stdout.strip()
        json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', raw_response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
        else:
            json_str = raw_response

        try:
            # OPT-037: Parse JSON from extracted content
            response_data = json.loads(json_str)
        except json.JSONDecodeError as e:
            # OPT-037: Parse failure - transition to pending_review
            result['status'] = 'error'
            result['error'] = f'JSON parse failed: {e}'

            # OPT-037a: Store raw response in metadata
            failure_metadata = json.loads(rule['metadata'] or '{}')
            failure_metadata['parse_failure'] = raw_response[:500]
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(failure_metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return result

        # Validate response
        validation_result = validate_response(response_data, vocab)
        if validation_result:
            # OPT-033, OPT-033a, OPT-033b: Validation failure
            result['status'] = 'validation_failed'
            result['error'] = validation_result['error']

            failure_metadata = json.loads(rule['metadata'] or '{}')
            failure_metadata['validation_failure'] = validation_result['error']
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(failure_metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return result

        # Extract response fields
        suggested_tags = response_data.get('tags', [])
        suggested_domain = response_data.get('domain', rule['domain']) or 'general'
        confidence = response_data.get('confidence', 0.5)  # OPT-032a: Default 0.5
        reasoning = response_data.get('reasoning', '')

        result['tags'] = suggested_tags
        result['confidence'] = confidence
        result['reasoning'] = reasoning

        # OPT-050, OPT-051: Calculate coherence
        coherence = calculate_coherence(suggested_tags, suggested_domain, vocab)
        result['coherence'] = coherence

        # OPT-011: Auto-approve decision logic
        if auto_approve:
            confidence_threshold = 0.70  # OPT-011: Uniform threshold

            if confidence >= confidence_threshold and coherence >= 0.3:
                decision = 'approve'
                result['status'] = 'approved'
            elif confidence >= confidence_threshold and coherence < 0.3:
                decision = 'skip'
                result['status'] = 'skipped'
            else:
                decision = 'skip'
                result['status'] = 'skipped'
        else:
            # Interactive mode - mark as pending for user review
            decision = 'skip'
            result['status'] = 'skipped'

        # Apply decision
        if decision == 'approve':
            # OPT-028: Determine tags_state based on confidence
            if confidence >= 0.9:
                tags_state = 'curated'  # OPT-028a
            elif confidence >= 0.7:
                tags_state = 'refined'  # OPT-028b
            else:
                tags_state = 'pending_review'  # OPT-028c

            # OPT-028e: Build metadata JSON
            metadata = json.loads(rule['metadata'] or '{}')
            metadata['optimization_reasoning'] = reasoning
            metadata['tag_confidence'] = confidence
            metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

            # OPT-003: Update rule in database
            conn.execute(
                """UPDATE rules
                   SET tags = ?, domain = ?, tags_state = ?, metadata = ?
                   WHERE id = ?""",
                (json.dumps(suggested_tags), suggested_domain, tags_state,
                 json.dumps(metadata), rule['id'])
            )
            conn.commit()

            # OPT-039-041: Update vocabulary
            update_vocabulary(rule['id'], suggested_domain, suggested_tags, vocab_path)

        conn.close()
        return result

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        conn.close()
        return result


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, auto_approve):
    """Execute single optimization pass (OPT-057a)."""

    # Load vocabulary and template
    vocab = load_vocabulary(vocab_path)
    templates_dir = BASE_DIR / config['structure']['templates_dir']
    template_path = templates_dir / 'runtime-template-tag-optimization.txt'
    with open(template_path) as f:
        template = f.read()

    # OPT-055a: Database path from config['structure']['database_path']
    db_path = BASE_DIR / config['structure']['database_path']

    # OPT-062: Track vocabulary before pass
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # Initialize counters
    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    # OPT-044a, OPT-044b: Parallel execution with ThreadPoolExecutor
    tag_opt_config = config.get('build_config', {}).get('tag_optimization', {})
    max_workers = tag_opt_config.get('parallel_max_workers', 3)

    print(f"\nPass {pass_number + 1}: Processing {len(remaining_rules)} rules...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for rule in remaining_rules:
            future = executor.submit(
                optimize_single_rule,
                rule, template, vocab, vocab_path, db_path, auto_approve, config
            )
            futures.append(future)

        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()

            # Track metrics
            if result['status'] == 'approved':
                approved_rules.append(result)
                approved_confidences.append(result['confidence'])
            elif result['status'] == 'error' or result['status'] == 'validation_failed':
                error_count += 1
            else:
                skipped_count += 1

            # OPT-044d: Verbose progress output
            if auto_approve:
                status_icon = {
                    'approved': '✓',
                    'skipped': '⊘',
                    'error': '✗',
                    'validation_failed': '✗'
                }.get(result['status'], '?')

                confidence = result.get('confidence', 0.0)
                coherence = result.get('coherence', 0.0)

                # Rule header with blank line separator
                print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")

                # Decision with context explaining why skipped
                if result['status'] == 'approved':
                    print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")
                    print(f"    Decision: approved")
                elif result['status'] == 'skipped':
                    print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")
                    if confidence < 0.7:
                        print(f"    Decision: skipped (confidence < 0.70)")
                    elif coherence < 0.3:
                        print(f"    Decision: skipped (coherence < 0.3)")
                    else:
                        print(f"    Decision: skipped")
                elif result['status'] in ('error', 'validation_failed'):
                    if result.get('error'):
                        print(f"    Error: {result['error']}")

                # Tags with different labels based on approval status
                if result.get('tags'):
                    if result['status'] == 'approved':
                        print(f"    Approved Tags: {', '.join(result['tags'])}")
                    else:
                        print(f"    Suggested Tags: {', '.join(result['tags'])}")

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

    # OPT-061: Domain-level convergence
    any_domain_active = any(metrics['improvement_rate'] > 0.10 for metrics in domain_metrics.values())

    # OPT-063: Vocabulary saturation
    vocabulary_saturated = (new_tags_added < 3 and improvement_rate < 0.10)

    # OPT-065: Quality floor
    quality_floor_reached = (avg_confidence < 0.65)

    # OPT-049, OPT-070: Pass summary
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

    # OPT-057a: Return dict with 9 keys
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
    """Master convergence decision (OPT-067)."""

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


def get_database_statistics(db_path):
    """Get database statistics for empty-state reporting (OPT-073)."""
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
    """Optimize rule tags through Claude reasoning with vocabulary intelligence and human oversight"""

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Optimize rule tags with vocabulary-aware intelligence'
    )
    parser.add_argument(
        '--auto-approve',
        action='store_true',
        help='Auto-approve tags with confidence >= 0.70 and coherence >= 0.3'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rules to process'
    )
    parser.add_argument(
        '--state',
        default='needs_tags',
        help='Filter by tags_state (default: needs_tags)'
    )
    args = parser.parse_args()

    print("Context Engine - Tag Optimization")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # OPT-019b: Vocabulary path relative to BASE_DIR
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'

    # Load vocabulary
    vocab = load_vocabulary(vocab_path)

    # OPT-055a: Database path from config['structure']['database_path']
    db_path = BASE_DIR / config['structure']['database_path']

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-072, OPT-073: Empty database state reporting
    stats = get_database_statistics(db_path)
    if stats['needs_tags'] == 0:
        print("\nNo rules require tag optimization.")
        print("")
        print("Database state:")
        print(f"  Total rules: {stats['total']}")
        print(f"  Curated: {stats['curated']}")
        print(f"  Refined: {stats['refined']}")
        print(f"  Pending review: {stats['pending_review']}")
        print(f"  Needs tags: 0")
        print("")

        # Guidance message
        if stats['total'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules from chatlogs.")
        elif stats['curated'] + stats['refined'] > 0:
            print("All rules have been tagged. Use 'make tags-stats' to view tag distribution.")
        else:
            print("All pending rules require manual review. Run 'make tags-optimize' for interactive tagging.")

        conn.close()
        return 0

    # OPT-056: Iterative mode when --auto-approve without --limit
    if args.auto_approve and args.limit is None:
        # Multi-pass iterative optimization

        # OPT-057: Load configuration
        build_config_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'
        if build_config_path.exists():
            with open(build_config_path) as f:
                build_config = yaml.safe_load(f)
            config['build_config'] = build_config
        else:
            config['build_config'] = {}

        tag_opt_config = config['build_config'].get('tag_optimization', {})
        max_passes = tag_opt_config.get('convergence_max_passes', 5)

        # OPT-057b: Calculate corpus size
        corpus_size = len(conn.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'").fetchall())

        # OPT-058: Cost limit
        cost_limit = max(500, corpus_size * 0.5)

        print(f"\nMulti-pass iterative optimization")
        print(f"  Corpus size: {corpus_size} rules")
        print(f"  Cost limit: {cost_limit} LLM calls")
        print(f"  Max passes: {max_passes}")

        pass_number = 0
        total_llm_calls = 0
        prev_avg_confidence = None

        while pass_number < max_passes:
            # Query remaining rules
            remaining_rules = conn.execute(
                "SELECT * FROM rules WHERE tags_state = 'needs_tags'"
            ).fetchall()

            if len(remaining_rules) == 0:
                break

            if total_llm_calls >= cost_limit:
                print(f"\n⚠️ Cost limit reached ({total_llm_calls} / {cost_limit} calls)")
                break

            # Run optimization pass
            pass_results = run_optimization_pass(
                remaining_rules, pass_number, vocab_path, config, args.auto_approve
            )

            # OPT-065: Quality degradation detection
            if pass_number > 0 and prev_avg_confidence is not None:
                confidence_drop = prev_avg_confidence - pass_results['avg_confidence']
                if confidence_drop > 0.15:
                    print(f"\n⚠️ Warning: Confidence dropped by {confidence_drop:.2f}")

            prev_avg_confidence = pass_results['avg_confidence']

            # OPT-067: Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1
            total_llm_calls += len(remaining_rules)

        # Final summary
        final_stats = get_database_statistics(db_path)
        print(f"\n{'='*70}")
        print("Final State")
        print(f"{'='*70}")
        print(f"  Total rules: {final_stats['total']}")
        print(f"  Curated: {final_stats['curated']}")
        print(f"  Refined: {final_stats['refined']}")
        print(f"  Pending review: {final_stats['pending_review']}")
        print(f"  Needs tags: {final_stats['needs_tags']}")

        if final_stats['needs_tags'] > 0:
            print(f"\nNext Steps:")
            print(f"  {final_stats['needs_tags']} rules remain untagged")
            print(f"  Run 'make tags-optimize' for interactive review")

    else:
        # Single-pass or interactive mode
        print("\nSingle-pass mode not yet implemented.")
        print("Use --auto-approve without --limit for multi-pass optimization.")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
