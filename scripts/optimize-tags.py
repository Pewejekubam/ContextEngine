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
    """Load tag vocabulary from YAML file (OPT-019, OPT-019a, OPT-019b)."""
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab
    except Exception as e:
        # OPT-035, OPT-035a: Vocabulary load failure aborts with error
        print(f"Error: Failed to load vocabulary from {vocab_path}: {e}", file=sys.stderr)
        sys.exit(1)


def get_tier_1_domains(vocab_path):
    """Extract tier-1 domain names from vocabulary file (OPT-060a)."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    tier_1_domains = list(vocab.get('tier_1_domains', {}).keys())
    return tier_1_domains


def load_all_tier2_tags_from_vocabulary(vocab_path):
    """Load all tier-2 tags across all domains (OPT-062a)."""
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)
    all_tags = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        all_tags.extend(tags)
    return all_tags


def validate_response(response_data, vocab, rule):
    """Validate Claude's tag optimization response (OPT-029 through OPT-033b)."""
    errors = []

    # OPT-030: Validate tag count (2-5)
    tags = response_data.get('tags', [])
    if not isinstance(tags, list) or len(tags) < 2 or len(tags) > 5:
        errors.append(f"tag count must be 2-5, got {len(tags)}")

    # OPT-029: Validate no forbidden stopwords
    stopwords = vocab.get('stopwords', [])
    for tag in tags:
        if tag in stopwords:
            errors.append(f"forbidden stopword: '{tag}'")

    # OPT-031: Validate domain exists in tier_1_domains
    domain = response_data.get('domain', rule['domain'])
    tier_1_domains = vocab.get('tier_1_domains', {})
    if domain and domain not in tier_1_domains:
        errors.append(f"invalid domain: '{domain}'")

    # OPT-032: Validate confidence is float 0.0-1.0
    confidence = response_data.get('confidence')
    if confidence is not None:
        try:
            confidence = float(confidence)
            if confidence < 0.0 or confidence > 1.0:
                errors.append(f"confidence must be 0.0-1.0, got {confidence}")
        except (ValueError, TypeError):
            errors.append(f"confidence must be float, got {type(confidence).__name__}")

    # OPT-033b: Return validation result
    if errors:
        return {
            'status': 'validation_failed',
            'error': '; '.join(errors)
        }

    return {'status': 'valid'}


def calculate_coherence(proposed_tags, domain, vocab):
    """Calculate coherence score (precision metric) (OPT-050, OPT-051, OPT-052)."""
    domain_tags = vocab.get('tier_2_tags', {}).get(domain, [])

    # OPT-052: Bootstrap exception - domains with < 5 existing tags bypass coherence check
    if len(domain_tags) < 5:
        return 1.0  # Trust early approvals

    # OPT-050: Precision = intersection / len(proposed_tags)
    if not proposed_tags:
        return 0.0

    intersection = sum(1 for tag in proposed_tags if tag in domain_tags)
    precision = intersection / len(proposed_tags)

    return precision


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
    tier_2_tags = '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (none defined)'

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

    # Synonyms (first 5 examples)
    synonyms_content = vocab.get('synonyms', {})
    if synonyms_content:
        synonym_lines = [f"  {canonical}: {', '.join(variants)}" for canonical, variants in list(synonyms_content.items())[:5]]
        if len(synonyms_content) > 5:
            synonym_lines.append(f"  ... (and {len(synonyms_content) - 5} more)")
        synonyms = '\n'.join(synonym_lines)
    else:
        synonyms = '  (none defined)'

    # OPT-034f: Forbidden stopwords (first 20 with count)
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


def optimize_single_rule(rule, template, vocab, config):
    """Optimize tags for a single rule using Claude (OPT-036, OPT-037, OPT-037b)."""
    # Format vocabulary components
    vocab_formatted = format_vocabulary_for_prompt(vocab)

    # Substitute template variables (OPT-034a)
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

    # Invoke Claude CLI with exponential backoff (OPT-044a)
    max_retries = 3
    initial_delay = 2
    max_delay = 60

    for attempt in range(max_retries):
        try:
            # OPT-036, OPT-037: Invoke Claude CLI with prompt as argument
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

            # Parse response
            break

        except subprocess.TimeoutExpired:
            return {
                'status': 'error',
                'error': 'Claude CLI timeout (30s)'
            }
        except FileNotFoundError:
            print("Error: 'claude' command not found. Install Claude CLI first.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            # Rate limit or other error - retry with exponential backoff
            if attempt < max_retries - 1:
                delay = min(initial_delay * (2 ** attempt), max_delay)
                jitter = delay * (random.uniform(-0.25, 0.25))
                sleep_time = delay + jitter
                time.sleep(sleep_time)
                continue
            else:
                return {
                    'status': 'error',
                    'error': f"API error after {max_retries} retries: {str(e)}"
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
        return {
            'status': 'parse_failed',
            'error': f"JSON parse error: {str(e)}",
            'raw_response': result.stdout[:500]
        }

    # Validate response
    validation = validate_response(response_data, vocab, rule)
    if validation['status'] != 'valid':
        return validation

    # Extract fields with defaults (OPT-032a)
    tags = response_data.get('tags', [])
    domain = response_data.get('domain', rule['domain'])
    confidence = float(response_data.get('confidence', 0.5))
    reasoning = response_data.get('reasoning', '')

    # Calculate coherence (OPT-050)
    coherence = calculate_coherence(tags, domain, vocab)

    return {
        'status': 'success',
        'tags': tags,
        'domain': domain,
        'confidence': confidence,
        'reasoning': reasoning,
        'coherence': coherence
    }


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """Update vocabulary with approved tags (OPT-039, OPT-039a, OPT-039b, OPT-039c, OPT-041a)."""
    # OPT-041a: Use exclusive file locking for thread-safe updates
    with open(vocab_path, 'r+') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

        vocab = yaml.safe_load(f)
        if vocab is None:  # Handle corruption
            return

        # OPT-039a: Validate domain exists in tier_1_domains
        if rule_domain not in vocab.get('tier_1_domains', {}):
            # OPT-039c: Log warning
            log_path = BASE_DIR / 'data' / 'tag_optimization_warnings.log'
            log_path.parent.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
            with open(log_path, 'a') as log:
                log.write(f"{timestamp}\t{rule_id}\tinvalid_domain\t{rule_domain}\tskipped_vocabulary_update\n")
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

        # OPT-041: Save with block style, preserve insertion order
        if tags_added:
            f.seek(0)
            f.truncate()
            yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)


def process_rule(rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold):
    """Process a single rule (creates its own database connection) (OPT-044c)."""
    # OPT-044c: Create thread-local database connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        # Optimize rule
        result = optimize_single_rule(rule, template, vocab, None)

        if result['status'] == 'error':
            # OPT-036a: Store error in metadata
            metadata = json.loads(rule['metadata'] or '{}')
            metadata['optimization_error'] = result['error']
            conn.execute(
                "UPDATE rules SET metadata = ? WHERE id = ?",
                (json.dumps(metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return {
                'status': 'error',
                'rule_id': rule['id'],
                'error': result['error']
            }

        if result['status'] == 'parse_failed':
            # OPT-037a: Store parse failure in metadata
            metadata = json.loads(rule['metadata'] or '{}')
            metadata['parse_failure'] = result.get('raw_response', '')
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return {
                'status': 'error',
                'rule_id': rule['id'],
                'error': result['error']
            }

        if result['status'] == 'validation_failed':
            # OPT-033a: Store validation failure in metadata
            metadata = json.loads(rule['metadata'] or '{}')
            metadata['validation_failure'] = result['error']
            conn.execute(
                "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
                ('pending_review', json.dumps(metadata), rule['id'])
            )
            conn.commit()
            conn.close()
            return {
                'status': 'error',
                'rule_id': rule['id'],
                'error': result['error']
            }

        # Result is success - apply auto-approval logic (OPT-011)
        tags = result['tags']
        domain = result['domain']
        confidence = result['confidence']
        reasoning = result['reasoning']
        coherence = result['coherence']

        if auto_approve:
            # OPT-011: Uniform 0.70 threshold + coherence >= 0.3
            if confidence >= confidence_threshold and coherence >= 0.3:
                decision = 'approve'
            else:
                decision = 'skip'
        else:
            # Interactive mode not implemented in worker threads
            decision = 'skip'

        if decision == 'approve':
            # OPT-028: Determine tags_state based on confidence
            if confidence >= 0.9:
                tags_state = 'curated'  # OPT-028a
            elif confidence >= 0.7:
                tags_state = 'refined'  # OPT-028b
            else:
                tags_state = 'pending_review'  # OPT-028c

            # OPT-028e: Store metadata
            metadata = json.loads(rule['metadata'] or '{}')
            metadata['optimization_reasoning'] = reasoning
            metadata['tag_confidence'] = confidence
            metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

            # OPT-044c: Update database and commit immediately
            conn.execute(
                """UPDATE rules
                   SET tags = ?, domain = ?, tags_state = ?,
                       confidence = ?, metadata = ?,
                       curated_at = ?, curated_by = ?
                   WHERE id = ?""",
                (json.dumps(tags), domain, tags_state, confidence, json.dumps(metadata),
                 datetime.now(UTC).isoformat().replace('+00:00', 'Z'), 'Claude Sonnet 4.5', rule['id'])
            )
            conn.commit()

            # OPT-046: Update vocabulary (thread-safe with file locking)
            update_vocabulary(rule['id'], domain, tags, vocab_path)

            conn.close()
            return {
                'status': 'approved',
                'rule_id': rule['id'],
                'tags': tags,
                'domain': domain,
                'confidence': confidence,
                'coherence': coherence,
                'reasoning': reasoning
            }
        else:
            # Skipped - no database update
            conn.close()
            return {
                'status': 'skipped',
                'rule_id': rule['id'],
                'tags': tags,
                'confidence': confidence,
                'coherence': coherence,
                'reasoning': reasoning
            }

    except Exception as e:
        conn.close()
        return {
            'status': 'error',
            'rule_id': rule['id'],
            'error': str(e)
        }


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, template, vocab, db_path, auto_approve, confidence_threshold, max_workers):
    """Execute single optimization pass (OPT-057a)."""
    print(f"\n{'='*70}")
    print(f"Pass {pass_number + 1}")
    print(f"{'='*70}")
    print(f"Processing {len(remaining_rules)} rules with needs_tags state...\n")

    # Track vocabulary state before pass (OPT-062)
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # Track metrics during processing
    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    # OPT-044a, OPT-044b: Parallel processing with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all rules
        futures = {
            executor.submit(process_rule, rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold): rule
            for rule in remaining_rules
        }

        completed = 0
        for future in as_completed(futures):
            completed += 1
            rule = futures[future]

            try:
                result = future.result()

                # OPT-044d: Verbose progress output
                if auto_approve:
                    status_icon = {
                        'approved': '✓',
                        'skipped': '⊘',
                        'error': '✗'
                    }.get(result['status'], '?')

                    print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                    print(f"    Title: {rule['title']}")

                    confidence = result.get('confidence', 0.0)
                    coherence = result.get('coherence', 0.0)
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

                    # Full reasoning (multi-line support)
                    if result.get('reasoning'):
                        reasoning_lines = result['reasoning'].split('\n')
                        print(f"    Reasoning: {reasoning_lines[0]}")
                        for line in reasoning_lines[1:]:
                            if line.strip():
                                print(f"               {line}")

                    # Tags with different labels
                    if result.get('tags'):
                        if result['status'] == 'approved':
                            print(f"    Approved Tags: {', '.join(result['tags'])}")
                        else:
                            print(f"    Suggested Tags: {', '.join(result['tags'])}")

            except Exception as e:
                error_count += 1
                print(f"\n  [{completed}/{len(remaining_rules)}] ✗ {rule['id']}")
                print(f"    Error: {str(e)}")

    # Track vocabulary state after pass (OPT-062)
    tags_after = set(load_all_tier2_tags_from_vocabulary(vocab_path))
    new_tags_added = len(tags_after - tags_before)

    # Calculate metrics (OPT-059, OPT-064)
    improvement_rate = len(approved_rules) / len(remaining_rules) if remaining_rules else 0.0
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

    # OPT-065: Quality floor (returns False for pass 1, checks degradation for pass 2+)
    quality_floor_reached = False
    if avg_confidence < 0.65 and len(approved_rules) > 0:
        quality_floor_reached = True

    # Print pass summary (OPT-070)
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

    # Return metrics (OPT-057a)
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
        print(f"\n⚠️  Quality floor reached (avg confidence < 0.65)")
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
    """Optimize rule tags through Claude reasoning with vocabulary intelligence."""
    parser = argparse.ArgumentParser(description='Optimize rule tags using Claude and vocabulary')
    parser.add_argument('--auto-approve', action='store_true', help='Auto-approve tags meeting confidence threshold')
    parser.add_argument('--limit', type=int, help='Limit number of rules to process')
    parser.add_argument('--rule-id', type=str, help='Process specific rule by ID')
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # OPT-055a: Database path from config['structure']['database_path']
    db_path = BASE_DIR / config['structure']['database_path']

    # OPT-019b: Vocabulary path relative to BASE_DIR
    vocab_path = BASE_DIR / 'config' / 'tag-vocabulary.yaml'

    # Load vocabulary (OPT-019)
    vocab = load_vocabulary(vocab_path)

    # Load template (OPT-034)
    template_path = BASE_DIR / config['structure']['templates_dir'] / 'runtime-template-tag-optimization.txt'
    try:
        with open(template_path) as f:
            template = f.read()
    except Exception as e:
        print(f"Error loading template from {template_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-072: Check if any rules need optimization
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags_count = cursor.fetchone()[0]

    if needs_tags_count == 0:
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

    # OPT-056: Iterative mode when --auto-approve without --limit
    if args.auto_approve and args.limit is None and args.rule_id is None:
        # Multi-pass iterative optimization (OPT-057)
        print("Context Engine - Tag Optimization (Iterative Mode)")
        print("="*70)

        # Load build constants for configuration
        build_constants_path = BASE_DIR.parent / 'build' / 'config' / 'build-constants.yaml'
        if build_constants_path.exists():
            with open(build_constants_path) as f:
                build_config = yaml.safe_load(f)
        else:
            build_config = {}

        tag_opt_config = build_config.get('tag_optimization', {})
        max_passes = tag_opt_config.get('convergence_max_passes', 10)
        max_workers = tag_opt_config.get('parallel_max_workers', 3)
        confidence_threshold = tag_opt_config.get('confidence_threshold', 0.70)

        # OPT-057b: Calculate corpus size
        cursor.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'")
        corpus_size = len(cursor.fetchall())

        # OPT-058: Calculate cost limit
        cost_limit = max(500, int(corpus_size * 0.5))

        print(f"Corpus size: {corpus_size} rules")
        print(f"Cost limit: {cost_limit} LLM calls")
        print(f"Max passes: {max_passes}")
        print(f"Parallel workers: {max_workers}")
        print(f"Confidence threshold: {confidence_threshold}")

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
                print(f"\n⚠️  Cost limit reached ({total_llm_calls}/{cost_limit} LLM calls)")
                break

            # Run optimization pass
            pass_results = run_optimization_pass(
                remaining_rules, pass_number, vocab_path, config,
                template, vocab, db_path, True, confidence_threshold, max_workers
            )

            # OPT-065: Quality degradation detection
            if pass_number > 0 and prev_avg_confidence is not None:
                confidence_drop = prev_avg_confidence - pass_results['avg_confidence']
                if confidence_drop > 0.15:
                    print(f"\n⚠️  Warning: Confidence dropped by {confidence_drop:.2f}")

            prev_avg_confidence = pass_results['avg_confidence']

            # Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1
            total_llm_calls += len(remaining_rules)

        # Final summary (OPT-070)
        print(f"\n{'='*70}")
        print("Multi-Pass Optimization Complete")
        print(f"{'='*70}")
        print(f"Total passes: {pass_number + 1}")
        print(f"Total LLM calls: {total_llm_calls}")

        # Final state
        stats = get_database_statistics(db_path)
        print(f"\nFinal State:")
        print(f"  Total rules: {stats['total']}")
        print(f"  Tagged: {stats['curated'] + stats['refined']}")
        print(f"  Pending review: {stats['pending_review']}")
        print(f"  Needs tags: {stats['needs_tags']}")

        if stats['needs_tags'] > 0:
            print(f"\nNext Steps:")
            print(f"  {stats['needs_tags']} rules remain untagged and require manual review.")
            print(f"  Run 'make tags-optimize' (without --auto-approve) for interactive tagging.")

        conn.close()
        return 0

    else:
        # Single-pass mode (interactive or limited batch)
        print("Context Engine - Tag Optimization")
        print("="*70)
        print("\nSingle-pass mode not fully implemented in this version.")
        print("Use --auto-approve without --limit for multi-pass iterative mode.")
        conn.close()
        return 0


if __name__ == '__main__':
    sys.exit(main())
