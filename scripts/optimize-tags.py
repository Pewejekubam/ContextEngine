#!/usr/bin/env python3
"""
Tag optimization with vocabulary-aware intelligence and HITL workflow

Implements constraints: OPT-001 through OPT-074
Generated from: specs/modules/runtime-script-tag-optimization-v1.5.11.yaml
"""

import sys
import json
import sqlite3
import subprocess
import argparse
import re
import time
import random
import fcntl
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
    """Load deployment configuration and vocabulary."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def load_vocabulary():
    """Load tag vocabulary from config/tag-vocabulary.yaml (OPT-019, OPT-019b)"""
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'

    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab, vocab_path
    except Exception as e:
        # OPT-035, OPT-035a
        print(f"Error loading vocabulary from {vocab_path}: {e}", file=sys.stderr)
        sys.exit(1)


def get_tier_1_domains(vocab_path):
    """Extract tier-1 domain names from vocabulary file (OPT-060a)"""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    tier_1_domains = list(vocab.get('tier_1_domains', {}).keys())
    return tier_1_domains


def load_all_tier2_tags_from_vocabulary(vocab_path):
    """Load all tier-2 tags from vocabulary file across all domains (OPT-062a)"""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    all_tags = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        all_tags.extend(tags)
    return all_tags


def format_vocabulary_for_prompt(vocab):
    """Format vocabulary components per OPT-034c through OPT-034f"""
    # OPT-034c: Tier-1 domains as comma-separated list
    tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

    # OPT-034d: Tier-2 tags with first 10 shown
    tier_2_tags_lines = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if len(tags) <= 10:
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
        else:
            remaining = len(tags) - 10
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
    tier_2_tags = '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (no tags yet)'

    # OPT-034e: Vocabulary mappings - first 5 examples
    mappings = vocab.get('vocabulary_mappings', {})
    if mappings and len(mappings) > 0:
        mapping_items = list(mappings.items())[:5]
        mapping_lines = [f'  "{word}" → {canonical}' for word, canonical in mapping_items]
        if len(mappings) > 5:
            mapping_lines.append(f"  ... (and {len(mappings) - 5} more)")
        vocabulary_mappings = '\n'.join(mapping_lines)
    else:
        vocabulary_mappings = '  (none defined)'

    # Synonyms formatting
    synonyms_content = vocab.get('synonyms', {})
    if synonyms_content:
        synonym_lines = [f"  {canonical}: {', '.join(variants)}" for canonical, variants in list(synonyms_content.items())[:5]]
        if len(synonyms_content) > 5:
            synonym_lines.append(f"  ... (and {len(synonyms_content) - 5} more)")
        synonyms = '\n'.join(synonym_lines)
    else:
        synonyms = '  (none defined)'

    # OPT-034f: Forbidden stopwords - first 20 with count
    stopwords = vocab.get('stopwords', [])
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


def validate_response(response_data, vocab):
    """Validate Claude response per OPT-029 through OPT-033b"""
    errors = []

    # OPT-030: Tag count validation
    tags = response_data.get('tags', [])
    if not (2 <= len(tags) <= 5):
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Forbidden stopwords validation
    stopwords = vocab.get('stopwords', [])
    forbidden_found = [tag for tag in tags if tag in stopwords]
    if forbidden_found:
        errors.append(f"forbidden stopwords found: {', '.join(forbidden_found)}")

    # OPT-031: Domain validation
    domain = response_data.get('domain')
    if domain and domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"invalid domain '{domain}', not in tier_1_domains")

    # OPT-032: Confidence validation
    confidence = response_data.get('confidence')
    if confidence is not None:
        if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
            errors.append(f"confidence must be float 0.0-1.0, got {confidence}")

    # OPT-033b: Return validation result
    if errors:
        return {
            'status': 'validation_failed',
            'error': '; '.join(errors)
        }
    return None


def calculate_coherence(proposed_tags, domain, vocab):
    """Calculate coherence as precision metric (OPT-050)"""
    domain_tags = vocab.get('tier_2_tags', {}).get(domain, [])

    # OPT-052: Bootstrap exception - domains with < 5 tags bypass coherence
    if len(domain_tags) < 5:
        return 1.0  # Trust early approvals

    # OPT-050: Precision = intersection / len(proposed_tags)
    intersection = sum(1 for tag in proposed_tags if tag in domain_tags)
    precision = intersection / len(proposed_tags) if proposed_tags else 0.0

    return precision


def optimize_single_rule(rule, template, vocab, vocab_path, db_path, auto_approve):
    """Optimize tags for a single rule using Claude CLI (OPT-036, OPT-037)"""

    # Create thread-local database connection (OPT-044c)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        # Format prompt from template
        vocab_formatted = format_vocabulary_for_prompt(vocab)

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
            session_context=''  # OPT-038: Not required
        )

        # Invoke Claude CLI (OPT-036, OPT-037)
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
                error_msg = result.stderr[:200]
                print(f"✗ Claude CLI failed for {rule['id']}: {error_msg}", file=sys.stderr)

                # OPT-036a: Store error in metadata
                error_metadata = json.loads(rule['metadata'] or '{}')
                error_metadata['optimization_error'] = error_msg
                conn.execute(
                    "UPDATE rules SET metadata = ? WHERE id = ?",
                    (json.dumps(error_metadata), rule['id'])
                )
                conn.commit()
                conn.close()
                return {
                    'status': 'error',
                    'rule_id': rule['id'],
                    'error': error_msg
                }

        except subprocess.TimeoutExpired:
            # OPT-036: Timeout is a CLI failure
            print(f"✗ Claude CLI timeout for {rule['id']}", file=sys.stderr)
            conn.close()
            return {
                'status': 'error',
                'rule_id': rule['id'],
                'error': 'timeout'
            }
        except FileNotFoundError:
            print("Error: 'claude' command not found. Install Claude CLI first.", file=sys.stderr)
            conn.close()
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
        except json.JSONDecodeError as e:
            # OPT-037: Parse failure - transition to pending_review
            print(f"✗ JSON parse failed for {rule['id']}: {e}", file=sys.stderr)

            # OPT-037a: Store raw response in metadata
            failure_metadata = json.loads(rule['metadata'] or '{}')
            failure_metadata['parse_failure'] = raw_response[:500]
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(failure_metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return {
                'status': 'error',
                'rule_id': rule['id'],
                'error': f'parse_failure: {str(e)}'
            }

        # Validate response (OPT-029 through OPT-033b)
        validation_result = validate_response(response_data, vocab)
        if validation_result:
            # OPT-033: Validation failures transition to pending_review
            # OPT-033a: Store validation failure in metadata
            failure_metadata = json.loads(rule['metadata'] or '{}')
            failure_metadata['validation_failure'] = validation_result['error']
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(failure_metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return {
                'status': 'validation_failed',
                'rule_id': rule['id'],
                'error': validation_result['error']
            }

        # Extract response fields
        suggested_tags = response_data.get('tags', [])
        suggested_domain = response_data.get('domain', rule['domain'] or 'general')
        confidence = response_data.get('confidence', 0.5)  # OPT-032a: Default 0.5
        reasoning = response_data.get('reasoning', '')

        # OPT-050: Calculate coherence
        coherence = calculate_coherence(suggested_tags, suggested_domain, vocab)

        # OPT-011: Auto-approve decision (uniform 0.70 threshold)
        if auto_approve:
            confidence_threshold = 0.70
            coherence_threshold = 0.30

            if confidence >= confidence_threshold and coherence >= coherence_threshold:
                decision = 'approve'
            else:
                decision = 'skip'
        else:
            # Interactive mode (not implemented in this version)
            decision = 'skip'

        # If approved, update database
        if decision == 'approve':
            # OPT-028: Determine tags_state based on confidence
            if confidence >= 0.9:
                tags_state = 'curated'  # OPT-028a
            elif confidence >= 0.7:
                tags_state = 'refined'  # OPT-028b
            else:
                tags_state = 'pending_review'  # OPT-028c

            # OPT-028e: Build metadata
            metadata = json.loads(rule['metadata'] or '{}')
            metadata['optimization_reasoning'] = reasoning
            metadata['tag_confidence'] = confidence
            metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

            # OPT-003: Update rule
            conn.execute(
                """UPDATE rules SET
                   tags = ?,
                   domain = ?,
                   tags_state = ?,
                   metadata = ?,
                   curated_at = ?,
                   curated_by = ?
                   WHERE id = ?""",
                (
                    json.dumps(suggested_tags),
                    suggested_domain,
                    tags_state,
                    json.dumps(metadata),
                    datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                    'Claude Sonnet 4.5',
                    rule['id']
                )
            )
            conn.commit()  # OPT-044c: Immediate commit

            # OPT-039: Update vocabulary with approved tags
            update_vocabulary(rule['id'], suggested_domain, suggested_tags, vocab_path)

            conn.close()
            return {
                'status': 'approved',
                'rule_id': rule['id'],
                'tags': suggested_tags,
                'domain': suggested_domain,
                'confidence': confidence,
                'coherence': coherence,
                'reasoning': reasoning,
                'tags_state': tags_state
            }
        else:
            # Skipped
            conn.close()
            return {
                'status': 'skipped',
                'rule_id': rule['id'],
                'tags': suggested_tags,
                'confidence': confidence,
                'coherence': coherence,
                'reasoning': reasoning
            }

    except Exception as e:
        conn.close()
        print(f"✗ Unexpected error processing {rule['id']}: {e}", file=sys.stderr)
        return {
            'status': 'error',
            'rule_id': rule['id'],
            'error': str(e)
        }


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """Update vocabulary with approved tags (OPT-039, OPT-041a)"""

    # OPT-041a: Exclusive file locking for thread-safe updates
    try:
        with open(vocab_path, 'r+') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # Block until lock acquired

            vocab = yaml.safe_load(f)
            if vocab is None:  # Handle corruption
                print(f"  ⚠ Warning: Vocabulary file corrupted, skipping update", file=sys.stderr)
                return

            # OPT-039a: Validate domain exists in tier_1_domains
            if rule_domain not in vocab.get('tier_1_domains', {}):
                # OPT-039c: Log warning
                log_path = BASE_DIR / 'data' / 'tag_optimization_warnings.log'
                log_path.parent.mkdir(exist_ok=True)
                timestamp = datetime.now(UTC).isoformat()
                with open(log_path, 'a') as log:
                    log.write(f"{timestamp}\t{rule_id}\tinvalid_domain\t{rule_domain}\tskipped_vocabulary_update\n")

                print(f"  ⚠ Warning: Invalid domain '{rule_domain}' for {rule_id}, skipping vocabulary update")
                return  # Skip vocabulary update

            # OPT-039b: Ensure tier_2_tags entry exists
            if 'tier_2_tags' not in vocab:
                vocab['tier_2_tags'] = {}
            if rule_domain not in vocab['tier_2_tags']:
                vocab['tier_2_tags'][rule_domain] = []

            # OPT-039, OPT-040: Append new tags
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
        print(f"  ⚠ Warning: Failed to update vocabulary: {e}", file=sys.stderr)


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, db_path, auto_approve):
    """Execute single optimization pass (OPT-057a)"""

    # Load template
    template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'
    with open(template_path) as f:
        template = f.read()

    # Load vocabulary (fresh for each pass to get updates)
    vocab, _ = load_vocabulary()

    # Track vocabulary state before pass (OPT-062)
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # Get tier-1 domains (OPT-060a)
    tier_1_domains = get_tier_1_domains(vocab_path)

    # Process rules in parallel (OPT-044, OPT-044a, OPT-044b)
    max_workers = config.get('tag_optimization', {}).get('max_workers', 3)

    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                optimize_single_rule,
                rule,
                template,
                vocab,
                vocab_path,
                db_path,
                auto_approve
            ): rule for rule in remaining_rules
        }

        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()

            # Track results
            if result['status'] == 'approved':
                approved_rules.append(result)
                approved_confidences.append(result['confidence'])
            elif result['status'] == 'error':
                error_count += 1
            elif result['status'] == 'skipped':
                skipped_count += 1

            # OPT-044d: Verbose progress output
            if auto_approve:
                status_icon = {
                    'approved': '✓',
                    'skipped': '⊘',
                    'error': '✗'
                }.get(result['status'], '?')

                rule = futures[future]
                confidence = result.get('confidence', 0.0)
                coherence = result.get('coherence', 0.0)

                print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                print(f"    Title: {rule['title']}")
                print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")

                # Decision with context
                if result['status'] == 'approved':
                    print(f"    Decision: approved")
                elif result['status'] == 'skipped':
                    if confidence < 0.70:
                        print(f"    Decision: skipped (confidence < 0.70)")
                    elif coherence < 0.30:
                        print(f"    Decision: skipped (coherence < 0.30)")
                    else:
                        print(f"    Decision: skipped")
                elif result['status'] == 'error':
                    print(f"    Decision: error")
                    if result.get('error'):
                        print(f"    Error: {result['error']}")

                # Full reasoning (multi-line)
                if result.get('reasoning'):
                    reasoning_lines = result['reasoning'].split('\n')
                    print(f"    Reasoning: {reasoning_lines[0]}")
                    for line in reasoning_lines[1:]:
                        if line.strip():
                            print(f"               {line}")

                # Tags with label based on status
                if result.get('tags'):
                    if result['status'] == 'approved':
                        print(f"    Approved Tags: {', '.join(result['tags'])}")
                    else:
                        print(f"    Suggested Tags: {', '.join(result['tags'])}")

    # Track vocabulary state after pass (OPT-062)
    tags_after = set(load_all_tier2_tags_from_vocabulary(vocab_path))
    new_tags_added = len(tags_after - tags_before)

    # OPT-059: Calculate improvement rate
    improvement_rate = len(approved_rules) / len(remaining_rules) if remaining_rules else 0.0

    # OPT-064: Calculate average confidence
    avg_confidence = sum(approved_confidences) / len(approved_confidences) if approved_confidences else 0.0

    # OPT-060: Domain-specific metrics (optimized)
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

    # OPT-065: Quality floor check
    quality_floor_reached = (avg_confidence < 0.65) if approved_confidences else False

    # Print pass summary (OPT-070 enhancement)
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

    # OPT-057a: Return metrics dict
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
    """Master convergence decision (OPT-067)"""

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
    """Get database statistics for empty-state reporting (OPT-073)"""
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

    parser = argparse.ArgumentParser(description='Optimize rule tags using Claude CLI')
    parser.add_argument('--auto-approve', action='store_true',
                       help='Auto-approve tags meeting confidence threshold (OPT-011)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of rules to process')
    args = parser.parse_args()

    print("Context Engine - Tag Optimization")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Load vocabulary
    vocab, vocab_path = load_vocabulary()

    # OPT-055a: Database path from config['structure']['database_path']
    db_path = BASE_DIR / config['structure']['database_path']

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-072: Check if any rules need optimization
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags_count = cursor.fetchone()[0]

    if needs_tags_count == 0:
        # OPT-072, OPT-073: Report database state
        stats = get_database_statistics(db_path)

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

    # OPT-056: Check for iterative mode (--auto-approve without --limit)
    if args.auto_approve and not args.limit:
        # Multi-pass iterative optimization (OPT-057)
        tag_opt_config = config.get('tag_optimization', {})
        max_passes = tag_opt_config.get('convergence_max_passes', 10)

        # OPT-057b: Calculate corpus size
        cursor.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'")
        corpus_size = len(cursor.fetchall())

        # OPT-058: Cost limit
        cost_limit = max(500, corpus_size * 0.5)

        print(f"\nIterative optimization mode:")
        print(f"  Corpus size: {corpus_size} rules")
        print(f"  Cost limit: {int(cost_limit)} LLM calls")
        print(f"  Max passes: {max_passes}")
        print("")

        pass_number = 0
        total_llm_calls = 0
        prev_avg_confidence = None

        while pass_number < max_passes:
            # Query remaining rules
            cursor.execute("SELECT * FROM rules WHERE tags_state = 'needs_tags'")
            remaining_rules = cursor.fetchall()

            if len(remaining_rules) == 0:
                break

            if total_llm_calls >= cost_limit:
                print(f"\n⚠️ Cost limit reached ({total_llm_calls} >= {cost_limit} calls)")
                break

            print(f"\n{'='*70}")
            print(f"Pass {pass_number + 1}: Processing {len(remaining_rules)} rules")
            print(f"{'='*70}")

            # Run pass
            pass_results = run_optimization_pass(
                remaining_rules,
                pass_number,
                vocab_path,
                config,
                db_path,
                args.auto_approve
            )

            # OPT-065: Quality degradation check
            if pass_number > 0 and prev_avg_confidence is not None:
                confidence_drop = prev_avg_confidence - pass_results['avg_confidence']
                if confidence_drop > 0.15:
                    print(f"\n⚠️ Warning: Confidence dropped by {confidence_drop:.2f}")

            prev_avg_confidence = pass_results['avg_confidence']

            # Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1
            total_llm_calls += len(remaining_rules)

        # Final summary
        print(f"\n{'='*70}")
        print("Optimization Complete")
        print(f"{'='*70}")
        print(f"  Total passes: {pass_number + 1}")
        print(f"  Total LLM calls: {total_llm_calls}")

        # Final state
        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state IN ('curated', 'refined')")
        tagged_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
        remaining = cursor.fetchone()[0]

        print(f"")
        print(f"Final state:")
        print(f"  Tagged: {tagged_count} rules")
        print(f"  Remaining: {remaining} rules")

        if remaining > 0:
            print(f"")
            print(f"Next steps:")
            print(f"  - Run 'make tags-optimize' for interactive review of {remaining} remaining rules")
            print(f"  - Or adjust confidence threshold in config and re-run")

    else:
        # Single-pass mode (with optional limit)
        limit = args.limit or None
        query = "SELECT * FROM rules WHERE tags_state = 'needs_tags'"
        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        rules = cursor.fetchall()

        print(f"\nProcessing {len(rules)} rules...")

        # Run single pass
        pass_results = run_optimization_pass(
            rules,
            0,
            vocab_path,
            config,
            db_path,
            args.auto_approve
        )

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
