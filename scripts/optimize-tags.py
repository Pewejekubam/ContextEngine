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
    """Load tag vocabulary from file (OPT-019)."""
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab
    except Exception as e:
        # OPT-035, OPT-035a: Vocabulary load failure
        print(f"Error: Failed to load vocabulary from {vocab_path}: {e}", file=sys.stderr)
        sys.exit(1)


def get_tier_1_domains(vocab_path):
    """Extract tier-1 domain names from vocabulary (OPT-060a)."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    tier_1_domains = list(vocab.get('tier_1_domains', {}).keys())
    return tier_1_domains


def load_all_tier2_tags_from_vocabulary(vocab_path):
    """Load all tier-2 tags from vocabulary across all domains (OPT-062a)."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    all_tags = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        all_tags.extend(tags)
    return all_tags


def format_vocabulary_for_prompt(vocab):
    """Format vocabulary components for Claude prompt (OPT-034c through OPT-034f)."""
    # OPT-034c: Tier-1 domains as comma-separated list
    tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

    # OPT-034d: Tier-2 tags with ellipsis for long lists
    tier_2_tags_lines = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if len(tags) <= 10:
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
        else:
            remaining = len(tags) - 10
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
    tier_2_tags = '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (empty)'

    # OPT-034e: Vocabulary mappings (first 5 examples)
    mappings = vocab.get('vocabulary_mappings', {})
    if mappings:
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

    # OPT-034f: Forbidden stopwords (first 20)
    forbidden = vocab.get('forbidden', {})
    stopwords = forbidden.get('stopwords', [])
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


def validate_response(response_data, vocab, rule):
    """Validate Claude's response against vocabulary constraints (OPT-029 through OPT-033b)."""
    errors = []

    # Extract fields
    tags = response_data.get('tags', [])
    domain = response_data.get('domain', rule['domain'])
    confidence = response_data.get('confidence')

    # OPT-030: Validate tag count (2-5)
    if not (2 <= len(tags) <= 5):
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Validate against forbidden stopwords
    forbidden_stopwords = vocab.get('forbidden', {}).get('stopwords', [])
    forbidden_tags = [tag for tag in tags if tag in forbidden_stopwords]
    if forbidden_tags:
        errors.append(f"forbidden stopwords: {', '.join(forbidden_tags)}")

    # OPT-031: Validate domain
    if domain and domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"invalid domain '{domain}' not in tier_1_domains")

    # OPT-032: Validate confidence score
    if confidence is None:
        # OPT-032a: Default to 0.5
        response_data['confidence'] = 0.5
    elif not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        # Invalid confidence, default to 0.5
        response_data['confidence'] = 0.5

    return errors


def calculate_coherence(proposed_tags, vocab, domain):
    """Calculate coherence as precision metric (OPT-050)."""
    if not proposed_tags:
        return 0.0

    domain_vocab = vocab.get('tier_2_tags', {}).get(domain, [])

    # OPT-052: Bootstrap exception - domains with < 5 tags bypass check
    if len(domain_vocab) < 5:
        return 1.0  # Trust early approvals

    # OPT-050: Precision = intersection / len(proposed_tags)
    intersection = sum(1 for tag in proposed_tags if tag in domain_vocab)
    precision = intersection / len(proposed_tags)

    return precision


def optimize_single_rule(rule, template, vocab, db_path):
    """Optimize tags for a single rule using Claude CLI (OPT-036, OPT-037)."""
    # Format vocabulary
    vocab_formatted = format_vocabulary_for_prompt(vocab)

    # Build prompt from template
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

    # OPT-036: Invoke Claude CLI (prompt as argument, not stdin)
    try:
        result = subprocess.run(
            ['claude', '--print', prompt],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL
        )

        if result.returncode != 0:
            # CLI failure
            return {
                'status': 'error',
                'rule_id': rule['id'],
                'error': result.stderr[:200]
            }
    except subprocess.TimeoutExpired:
        return {
            'status': 'error',
            'rule_id': rule['id'],
            'error': 'Claude CLI timeout (30s)'
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
    except json.JSONDecodeError as e:
        return {
            'status': 'parse_error',
            'rule_id': rule['id'],
            'error': f"JSON parse failed: {e}",
            'raw_response': raw_response[:500]
        }

    # Validate response
    validation_errors = validate_response(response_data, vocab, rule)
    if validation_errors:
        # OPT-033b: Return validation failure
        return {
            'status': 'validation_failed',
            'rule_id': rule['id'],
            'error': '; '.join(validation_errors)
        }

    # Extract validated fields
    suggested_tags = response_data.get('tags', [])
    suggested_domain = response_data.get('domain', rule['domain'] or 'general')
    confidence = response_data['confidence']
    reasoning = response_data.get('reasoning', '')

    # OPT-050: Calculate coherence
    coherence = calculate_coherence(suggested_tags, vocab, suggested_domain)

    return {
        'status': 'success',
        'rule_id': rule['id'],
        'tags': suggested_tags,
        'domain': suggested_domain,
        'confidence': confidence,
        'coherence': coherence,
        'reasoning': reasoning,
        'title': rule['title']
    }


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """Update vocabulary with approved tags (OPT-039 through OPT-041a)."""
    # OPT-041a: Use exclusive file locking
    with open(vocab_path, 'r+') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

        vocab = yaml.safe_load(f)
        if vocab is None:
            return  # Corrupted file

        # OPT-039a: Validate domain
        if rule_domain not in vocab.get('tier_1_domains', {}):
            # OPT-039c: Log warning
            print(f"  ⚠ Warning: Invalid domain '{rule_domain}' for {rule_id}, skipping vocabulary update")
            return

        # OPT-039b: Ensure tier_2_tags entry exists
        if 'tier_2_tags' not in vocab:
            vocab['tier_2_tags'] = {}
        if rule_domain not in vocab['tier_2_tags']:
            vocab['tier_2_tags'][rule_domain] = []

        # OPT-039, OPT-040: Append new tags only
        tags_added = False
        for tag in approved_tags:
            if tag not in vocab['tier_2_tags'][rule_domain]:
                vocab['tier_2_tags'][rule_domain].append(tag)
                tags_added = True

        # OPT-041: Save with block style
        if tags_added:
            f.seek(0)
            f.truncate()
            yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)


def process_rule_with_approval(rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold):
    """Process a single rule with approval logic (OPT-044c thread-safe)."""
    # Create thread-local connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Optimize tags
    result = optimize_single_rule(rule, template, vocab, db_path)

    if result['status'] == 'error':
        # OPT-036a: Store error in metadata
        error_metadata = json.loads(rule['metadata'] or '{}')
        error_metadata['optimization_error'] = result['error']
        conn.execute(
            "UPDATE rules SET metadata = ? WHERE id = ?",
            (json.dumps(error_metadata), rule['id'])
        )
        conn.commit()
        conn.close()
        return result

    if result['status'] == 'parse_error':
        # OPT-037a: Store raw response in metadata
        failure_metadata = json.loads(rule['metadata'] or '{}')
        failure_metadata['parse_failure'] = result.get('raw_response', '')
        conn.execute(
            "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
            ('pending_review', json.dumps(failure_metadata), rule['id'])
        )
        conn.commit()
        conn.close()
        return result

    if result['status'] == 'validation_failed':
        # OPT-033a: Store validation failure
        validation_metadata = json.loads(rule['metadata'] or '{}')
        validation_metadata['validation_failure'] = result['error']
        conn.execute(
            "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
            ('pending_review', json.dumps(validation_metadata), rule['id'])
        )
        conn.commit()
        conn.close()
        return result

    # Success - check auto-approval
    confidence = result['confidence']
    coherence = result['coherence']

    # OPT-011: Auto-approve logic
    if auto_approve:
        if confidence >= confidence_threshold and coherence >= 0.3:
            decision = 'approve'
        else:
            decision = 'skip'
            result['status'] = 'skipped'
    else:
        # Interactive prompt (OPT-010)
        print(f"\n{'='*70}")
        print(f"Rule: {result['rule_id']}")
        print(f"Title: {result['title']}")  # OPT-010b: No truncation
        print(f"Domain: {result['domain']}")
        print(f"Suggested tags: {', '.join(result['tags'])}")
        print(f"Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")
        print(f"Reasoning: {result['reasoning']}")  # OPT-010b: No truncation
        print()
        print("1. Approve")
        print("2. Skip")
        print("3. Quit")

        while True:
            choice = input("\nChoice [1-3]: ").strip()
            if choice == '1':
                decision = 'approve'
                break
            elif choice == '2':
                decision = 'skip'
                result['status'] = 'skipped'
                break
            elif choice == '3':
                # OPT-010a: Confirm quit
                confirm = input("Quit optimization? [y/n]: ").strip().lower()
                if confirm == 'y':
                    conn.close()
                    sys.exit(0)
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
        metadata['optimization_reasoning'] = result['reasoning']
        metadata['tag_confidence'] = confidence
        metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

        # Update database
        conn.execute(
            """UPDATE rules SET
               tags = ?,
               domain = ?,
               tags_state = ?,
               confidence = ?,
               metadata = ?,
               curated_at = ?,
               curated_by = ?
               WHERE id = ?""",
            (
                json.dumps(result['tags']),
                result['domain'],
                tags_state,
                confidence,
                json.dumps(metadata),
                datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                "Claude Sonnet 4.5",
                rule['id']
            )
        )
        conn.commit()

        # OPT-039: Update vocabulary
        update_vocabulary(rule['id'], result['domain'], result['tags'], vocab_path)

        result['status'] = 'approved'

    conn.close()
    return result


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, template, vocab, db_path, auto_approve, confidence_threshold):
    """Execute single optimization pass (OPT-057a)."""
    print(f"\n{'='*70}")
    print(f"Pass {pass_number + 1}")
    print(f"{'='*70}")
    print(f"Processing {len(remaining_rules)} rules...")

    # Track vocabulary before pass (OPT-062)
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # Get tier-1 domains for metrics (OPT-060a)
    tier_1_domains = get_tier_1_domains(vocab_path)

    # Reload vocabulary for this pass
    vocab = load_vocabulary(vocab_path)

    # OPT-044: Parallel processing
    tag_opt_config = config.get('tag_optimization', {})
    max_workers = tag_opt_config.get('parallel_max_workers', 3)

    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all rules
        futures = {
            executor.submit(
                process_rule_with_approval,
                rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold
            ): rule for rule in remaining_rules
        }

        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()

            # Track metrics
            if result['status'] == 'approved':
                approved_rules.append(result)
                approved_confidences.append(result['confidence'])
            elif result['status'] == 'error' or result['status'] == 'parse_error':
                error_count += 1
            elif result['status'] == 'skipped' or result['status'] == 'validation_failed':
                skipped_count += 1

            # OPT-044d: Verbose progress output (only in auto-approve mode)
            if auto_approve:
                status_icon = {
                    'approved': '✓',
                    'skipped': '⊘',
                    'error': '✗',
                    'parse_error': '✗',
                    'validation_failed': '⊘'
                }.get(result['status'], '?')

                confidence = result.get('confidence', 0.0)
                coherence = result.get('coherence', 0.0)

                print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                if result.get('title'):
                    print(f"    Title: {result['title']}")
                print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")

                # Decision with context
                if result['status'] == 'approved':
                    print(f"    Decision: approved")
                elif result['status'] == 'skipped':
                    if confidence < confidence_threshold:
                        print(f"    Decision: skipped (confidence < {confidence_threshold})")
                    elif coherence < 0.3:
                        print(f"    Decision: skipped (coherence < 0.3)")
                    else:
                        print(f"    Decision: skipped")
                elif result['status'] in ['error', 'parse_error', 'validation_failed']:
                    print(f"    Decision: {result['status']}")
                    if result.get('error'):
                        print(f"    Error: {result['error']}")

                # Full reasoning
                if result.get('reasoning'):
                    reasoning_lines = result['reasoning'].split('\n')
                    print(f"    Reasoning: {reasoning_lines[0]}")
                    for line in reasoning_lines[1:]:
                        if line.strip():
                            print(f"               {line}")

                # Tags
                if result.get('tags'):
                    if result['status'] == 'approved':
                        print(f"    Approved Tags: {', '.join(result['tags'])}")
                    else:
                        print(f"    Suggested Tags: {', '.join(result['tags'])}")

    # Track vocabulary after pass (OPT-062)
    tags_after = set(load_all_tier2_tags_from_vocabulary(vocab_path))
    new_tags_added = len(tags_after - tags_before)

    # OPT-059: Calculate improvement rate
    improvement_rate = len(approved_rules) / len(remaining_rules) if remaining_rules else 0.0

    # OPT-064: Calculate average confidence
    avg_confidence = sum(approved_confidences) / len(approved_confidences) if approved_confidences else 0.0

    # OPT-060: Domain metrics (optimized to avoid O(n²))
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
    any_domain_active = any(m['improvement_rate'] > 0.10 for m in domain_metrics.values())

    # OPT-063: Vocabulary saturation
    vocabulary_saturated = (new_tags_added < 3 and improvement_rate < 0.10)

    # OPT-065: Quality degradation detection
    quality_floor_reached = (avg_confidence < 0.65) if approved_confidences else False

    # Calculate remaining count
    conn = sqlite3.connect(str(db_path))
    remaining_count = conn.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'").fetchone()[0]
    conn.close()

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

    # OPT-057a: Return pass results
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
        print(f"\n⚠️ Quality floor reached (avg confidence <0.65)")
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
    """Optimize rule tags through Claude reasoning with vocabulary intelligence and human oversight."""
    parser = argparse.ArgumentParser(description='Optimize rule tags using Claude and vocabulary')
    parser.add_argument('--auto-approve', action='store_true', help='Auto-approve based on confidence threshold')
    parser.add_argument('--limit', type=int, help='Limit number of rules to process')
    parser.add_argument('--state', default='needs_tags', help='Filter by tags_state (default: needs_tags)')
    args = parser.parse_args()

    # Load configuration
    config = load_config()

    # Get paths
    db_path = BASE_DIR / config['structure']['database_path']
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')
    template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'

    # Load vocabulary and template
    vocab = load_vocabulary(vocab_path)
    with open(template_path) as f:
        template = f.read()

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-072: Check if any rules need optimization
    needs_tags_count = conn.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'").fetchone()[0]

    if needs_tags_count == 0:
        # Empty state reporting
        stats = get_database_statistics(db_path)
        print("No rules require tag optimization.")
        print()
        print("Database state:")
        print(f"  Total rules: {stats['total']}")
        print(f"  Curated: {stats['curated']}")
        print(f"  Refined: {stats['refined']}")
        print(f"  Pending review: {stats['pending_review']}")
        print(f"  Needs tags: 0")
        print()

        if stats['total'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules from chatlogs.")
        elif stats['curated'] + stats['refined'] > 0:
            print("All rules have been tagged. Use 'make tags-stats' to view tag distribution.")
        else:
            print("All pending rules require manual review. Run 'make tags-optimize' for interactive tagging.")

        conn.close()
        return 0

    # OPT-056: Check for iterative mode
    if args.auto_approve and not args.limit:
        # Multi-pass iterative mode (OPT-057)
        tag_opt_config = config.get('tag_optimization', {})
        max_passes = tag_opt_config.get('convergence_max_passes', 5)
        confidence_threshold = tag_opt_config.get('pass_1_threshold', 0.70)

        # OPT-057b: Calculate corpus size
        corpus_size = conn.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'").fetchone()[0]

        # OPT-058: Calculate cost limit
        cost_limit = max(500, int(corpus_size * 0.5))

        print(f"Multi-pass optimization mode")
        print(f"Corpus size: {corpus_size} rules")
        print(f"Cost limit: {cost_limit} LLM calls")
        print(f"Max passes: {max_passes}")
        print(f"Confidence threshold: {confidence_threshold}")

        pass_number = 0
        total_llm_calls = 0

        while pass_number < max_passes:
            # Query remaining rules
            remaining_rules = conn.execute(
                "SELECT * FROM rules WHERE tags_state = 'needs_tags'"
            ).fetchall()

            if len(remaining_rules) == 0:
                break

            if total_llm_calls >= cost_limit:
                print(f"\n⚠️ Cost limit reached ({cost_limit} LLM calls)")
                break

            # Run pass
            pass_results = run_optimization_pass(
                remaining_rules, pass_number, vocab_path, config,
                template, vocab, db_path, args.auto_approve, confidence_threshold
            )

            # Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1
            total_llm_calls += len(remaining_rules)

        # Final summary
        final_stats = get_database_statistics(db_path)
        print(f"\n{'='*70}")
        print("Optimization Complete")
        print(f"{'='*70}")
        print(f"Total passes: {pass_number + 1}")
        print(f"Total LLM calls: {total_llm_calls}")
        print(f"Final state:")
        print(f"  Curated: {final_stats['curated']}")
        print(f"  Refined: {final_stats['refined']}")
        print(f"  Pending review: {final_stats['pending_review']}")
        print(f"  Needs tags: {final_stats['needs_tags']}")

        if final_stats['needs_tags'] > 0:
            print(f"\nNext steps:")
            print(f"  {final_stats['needs_tags']} rules remain for manual review")
            print(f"  Run 'make tags-optimize' for interactive tagging")

    else:
        # Single-pass mode (original behavior)
        query = f"SELECT * FROM rules WHERE tags_state = ?"
        params = [args.state]

        if args.limit:
            query += " LIMIT ?"
            params.append(args.limit)

        rules = conn.execute(query, params).fetchall()

        if not rules:
            print(f"No rules found with tags_state = '{args.state}'")
            conn.close()
            return 0

        print(f"Processing {len(rules)} rules...")

        # Load build config for threshold
        build_config_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'
        if build_config_path.exists():
            with open(build_config_path) as f:
                build_config = yaml.safe_load(f)
            confidence_threshold = build_config.get('tag_optimization', {}).get('pass_1_threshold', 0.70)
        else:
            confidence_threshold = 0.70

        # Process all rules
        approved_count = 0
        skipped_count = 0
        error_count = 0

        for rule in rules:
            result = process_rule_with_approval(
                rule, template, vocab, vocab_path, db_path,
                args.auto_approve, confidence_threshold
            )

            if result['status'] == 'approved':
                approved_count += 1
            elif result['status'] in ['skipped', 'validation_failed']:
                skipped_count += 1
            else:
                error_count += 1

        # Summary
        print(f"\n{'='*70}")
        print("Optimization Summary")
        print(f"{'='*70}")
        print(f"Processed: {len(rules)}")
        print(f"Approved: {approved_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Errors: {error_count}")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
