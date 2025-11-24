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
import subprocess
import argparse
import re
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

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


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_vocabulary(vocab_path):
    """Load tag vocabulary from config/tag-vocabulary.yaml (OPT-019, OPT-019a)."""
    try:
        with open(vocab_path) as f:
            vocab = yaml.safe_load(f)
        return vocab
    except Exception as e:
        # OPT-035, OPT-035a: Vocabulary load failure aborts with error
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


def format_vocabulary_for_prompt(vocab):
    """Format vocabulary components for Claude prompt (OPT-034c through OPT-034f)."""
    # OPT-034c: Tier-1 domains as comma-separated list
    tier_1_domains = ', '.join(vocab.get('tier_1_domains', {}).keys())

    # OPT-034d: Tier-2 tags with first 10, ellipsis for remaining
    tier_2_tags_lines = []
    for domain, tags in vocab.get('tier_2_tags', {}).items():
        if len(tags) <= 10:
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags)}")
        else:
            remaining = len(tags) - 10
            tier_2_tags_lines.append(f"  {domain}: {', '.join(tags[:10])}, ... (and {remaining} more)")
    tier_2_tags = '\n'.join(tier_2_tags_lines) if tier_2_tags_lines else '  (none defined)'

    # OPT-034e: Vocabulary mappings with first 5 examples
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

    # OPT-034f: Forbidden stopwords with first 20
    forbidden = vocab.get('forbidden', {})
    stopwords = forbidden.get('stopwords', [])
    if len(stopwords) <= 20:
        forbidden_stopwords = ', '.join(stopwords)
    else:
        forbidden_stopwords = ', '.join(stopwords[:20]) + f", ... (and {len(stopwords) - 20} more)"

    return tier_1_domains, tier_2_tags, vocabulary_mappings, synonyms, forbidden_stopwords


def validate_response(response_data, vocab, rule):
    """Validate Claude's response (OPT-029 through OPT-033b)."""
    errors = []

    tags = response_data.get('tags', [])
    domain = response_data.get('domain', rule['domain'])
    confidence = response_data.get('confidence')

    # OPT-030: Validate tag count is between 2 and 5
    if not isinstance(tags, list) or len(tags) < 2 or len(tags) > 5:
        errors.append(f"tag count must be 2-5, got {len(tags) if isinstance(tags, list) else 0}")

    # OPT-029: Validate tags against forbidden stopwords
    if isinstance(tags, list):
        forbidden_stopwords = vocab.get('forbidden', {}).get('stopwords', [])
        for tag in tags:
            if tag in forbidden_stopwords:
                errors.append(f"tag '{tag}' is a forbidden stopword")

    # OPT-031: Validate domain exists in tier_1_domains
    if domain and domain not in vocab.get('tier_1_domains', {}):
        errors.append(f"domain '{domain}' not in tier_1_domains")

    # OPT-032: Validate confidence is float between 0.0 and 1.0
    if confidence is not None:
        try:
            conf_float = float(confidence)
            if conf_float < 0.0 or conf_float > 1.0:
                errors.append(f"confidence {conf_float} not in range 0.0-1.0")
        except (ValueError, TypeError):
            errors.append(f"confidence '{confidence}' is not a valid float")

    # OPT-033b: Return validation failure dict
    if errors:
        return {
            'status': 'validation_failed',
            'error': '; '.join(errors)
        }

    return None


def calculate_coherence(proposed_tags, domain, vocab):
    """Calculate coherence as precision metric (OPT-050)."""
    domain_tags = vocab.get('tier_2_tags', {}).get(domain, [])

    # OPT-052: Bootstrap exception - domains with < 5 tags bypass coherence check
    if len(domain_tags) < 5:
        return 1.0  # Bypass coherence check for bootstrap domains

    # OPT-050: Coherence = intersection / len(proposed_tags) (precision metric)
    intersection = sum(1 for tag in proposed_tags if tag in domain_tags)
    precision = intersection / len(proposed_tags) if proposed_tags else 0.0

    return precision


def update_vocabulary(rule_id, rule_domain, approved_tags, vocab_path):
    """Update vocabulary with approved tags (OPT-039 through OPT-041)."""
    # Load vocabulary
    with open(vocab_path) as f:
        vocab = yaml.safe_load(f)

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
    tags_added = []
    for tag in approved_tags:
        if tag not in vocab['tier_2_tags'][rule_domain]:
            vocab['tier_2_tags'][rule_domain].append(tag)
            tags_added.append(tag)

    # OPT-041: Save with block style, preserve insertion order
    if tags_added:
        with open(vocab_path, 'w') as f:
            yaml.dump(vocab, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)


def call_claude_for_optimization(rule, template, vocab, auto_approve=False):
    """Call Claude CLI to optimize tags for a single rule (OPT-036, OPT-037)."""
    # Format vocabulary components
    tier_1_domains, tier_2_tags, vocabulary_mappings, synonyms, forbidden_stopwords = format_vocabulary_for_prompt(vocab)

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

    # OPT-036: Invoke Claude CLI with prompt as argument (v1.5.10 fix)
    try:
        result = subprocess.run(
            ['claude', '--print', prompt],
            capture_output=True,
            text=True,
            timeout=30,
            stdin=subprocess.DEVNULL
        )

        if result.returncode != 0:
            # OPT-036: CLI failure - return error
            return {
                'status': 'error',
                'error': result.stderr[:200],
                'rule_id': rule['id']
            }

    except subprocess.TimeoutExpired:
        # OPT-036: Timeout is a CLI failure
        return {
            'status': 'error',
            'error': 'Claude CLI timeout',
            'rule_id': rule['id']
        }
    except FileNotFoundError:
        # Claude CLI not installed
        print("Error: 'claude' command not found. Install Claude CLI first.", file=sys.stderr)
        sys.exit(1)

    # OPT-037b: Extract JSON from markdown code blocks if present
    raw_response = result.stdout.strip()
    json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', raw_response, re.DOTALL)
    if json_match:
        json_str = json_match.group(1).strip()
    else:
        json_str = raw_response

    try:
        # OPT-037: Parse JSON
        response_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        # OPT-037: Parse failure
        return {
            'status': 'error',
            'error': f'JSON parse failed: {e}',
            'raw_response': result.stdout[:500],
            'rule_id': rule['id']
        }

    # Validate response
    validation_error = validate_response(response_data, vocab, rule)
    if validation_error:
        return {**validation_error, 'rule_id': rule['id']}

    # Extract response fields with defaults
    suggested_tags = response_data.get('tags', [])
    suggested_domain = response_data.get('domain', rule['domain'] or 'general')
    confidence = float(response_data.get('confidence', 0.5))  # OPT-032a: Default 0.5
    reasoning = response_data.get('reasoning', '')

    # Calculate coherence (OPT-050)
    coherence = calculate_coherence(suggested_tags, suggested_domain, vocab)

    return {
        'status': 'success',
        'rule_id': rule['id'],
        'tags': suggested_tags,
        'domain': suggested_domain,
        'confidence': confidence,
        'coherence': coherence,
        'reasoning': reasoning
    }


def process_rule(rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold):
    """Process a single rule (worker thread function) (OPT-044c)."""
    # OPT-044c: Create thread-local connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Call Claude API
    result = call_claude_for_optimization(rule, template, vocab, auto_approve)

    if result['status'] == 'error':
        # OPT-036a: Store CLI error in metadata
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['optimization_error'] = result.get('error', '')

        conn.execute(
            "UPDATE rules SET metadata = ? WHERE id = ?",
            (json.dumps(metadata), rule['id'])
        )
        conn.commit()
        conn.close()
        return {**result, 'decision': 'error'}

    if result['status'] == 'validation_failed':
        # OPT-033, OPT-033a: Validation failure - transition to pending_review
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['validation_failure'] = result.get('error', '')

        conn.execute(
            "UPDATE rules SET tags_state = ?, metadata = ? WHERE id = ?",
            ('pending_review', json.dumps(metadata), rule['id'])
        )
        conn.commit()
        conn.close()
        return {**result, 'decision': 'skipped'}

    # OPT-011: Auto-approve decision logic
    confidence = result['confidence']
    coherence = result['coherence']

    if auto_approve:
        # OPT-011: Uniform 0.70 threshold with coherence >= 0.3
        if confidence >= confidence_threshold and coherence >= 0.3:
            decision = 'approve'
        else:
            decision = 'skip'
    else:
        # Interactive mode would prompt user here (OPT-010)
        # For now, auto-approve behavior
        decision = 'approve' if confidence >= confidence_threshold and coherence >= 0.3 else 'skip'

    if decision == 'approve':
        # OPT-028: Determine tags_state based on confidence
        if confidence >= 0.9:
            tags_state = 'curated'  # OPT-028a
        elif confidence >= 0.7:
            tags_state = 'refined'  # OPT-028b
        else:
            tags_state = 'pending_review'  # OPT-028c

        # OPT-028e, OPT-028f: Update metadata
        metadata = json.loads(rule['metadata'] or '{}')
        metadata['optimization_reasoning'] = result['reasoning']
        metadata['tag_confidence'] = confidence
        metadata['optimized_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

        # OPT-003: Update rule
        conn.execute("""
            UPDATE rules
            SET tags = ?, domain = ?, tags_state = ?, confidence = ?,
                metadata = ?, curated_at = ?, curated_by = ?
            WHERE id = ?
        """, (
            json.dumps(result['tags']),
            result['domain'],
            tags_state,
            confidence,
            json.dumps(metadata),
            datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
            'Claude Sonnet 4.5',
            rule['id']
        ))

        # OPT-044c, OPT-056a: Commit immediately (per-rule atomicity)
        conn.commit()

        # OPT-039: Update vocabulary
        update_vocabulary(rule['id'], result['domain'], result['tags'], vocab_path)

        conn.close()
        return {**result, 'decision': 'approved'}
    else:
        # Skip - no database update
        conn.close()
        return {**result, 'decision': 'skipped'}


def run_optimization_pass(remaining_rules, pass_number, vocab_path, config, db_path, template, auto_approve, confidence_threshold, max_workers):
    """Execute single optimization pass (OPT-057a)."""
    # Load vocabulary (may have been updated by previous pass)
    vocab = load_vocabulary(vocab_path)

    # Track vocabulary before pass (OPT-062)
    tags_before = set(load_all_tier2_tags_from_vocabulary(vocab_path))

    # Get tier-1 domains for metrics (OPT-060a)
    tier_1_domains = get_tier_1_domains(vocab_path)

    # Initialize tracking
    approved_rules = []
    approved_confidences = []
    error_count = 0
    skipped_count = 0

    # OPT-044, OPT-044a: Parallel execution with ThreadPoolExecutor
    print(f"\nPass {pass_number + 1}: Processing {len(remaining_rules)} rules...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all rules for processing
        future_to_rule = {
            executor.submit(process_rule, rule, template, vocab, vocab_path, db_path, auto_approve, confidence_threshold): rule
            for rule in remaining_rules
        }

        completed = 0
        for future in as_completed(future_to_rule):
            rule = future_to_rule[future]
            result = future.result()
            completed += 1

            # Track results
            if result['decision'] == 'approved':
                approved_rules.append(result)
                approved_confidences.append(result['confidence'])
            elif result['decision'] == 'error':
                error_count += 1
            elif result['decision'] == 'skipped':
                skipped_count += 1

            # OPT-044d: Verbose progress output (enhanced version)
            if auto_approve:
                status_icon = {'approved': '✓', 'skipped': '⊘', 'error': '✗'}.get(result['decision'], '?')
                confidence = result.get('confidence', 0.0)
                coherence = result.get('coherence', 0.0)

                print(f"\n  [{completed}/{len(remaining_rules)}] {status_icon} {result['rule_id']}")
                print(f"    Title: {rule['title']}")
                print(f"    Confidence: {confidence:.2f} | Coherence: {coherence:.2f}")

                if result['decision'] == 'approved':
                    print(f"    Decision: approved")
                    print(f"    Approved Tags: {', '.join(result.get('tags', []))}")
                elif result['decision'] == 'skipped':
                    if confidence < confidence_threshold:
                        print(f"    Decision: skipped (confidence {confidence:.2f} < {confidence_threshold})")
                    elif coherence < 0.3:
                        print(f"    Decision: skipped (coherence {coherence:.2f} < 0.3)")
                    else:
                        print(f"    Decision: skipped")
                    if result.get('tags'):
                        print(f"    Suggested Tags: {', '.join(result.get('tags', []))}")
                elif result['decision'] == 'error':
                    print(f"    Decision: error")
                    if result.get('error'):
                        print(f"    Error: {result['error']}")

                if result.get('reasoning'):
                    reasoning_lines = result['reasoning'].split('\n')
                    print(f"    Reasoning: {reasoning_lines[0]}")
                    for line in reasoning_lines[1:]:
                        if line.strip():
                            print(f"               {line}")

    # Track vocabulary after pass (OPT-062)
    tags_after = set(load_all_tier2_tags_from_vocabulary(vocab_path))
    new_tags_count = len(tags_after - tags_before)

    # OPT-059: Calculate improvement rate
    improvement_rate = len(approved_rules) / len(remaining_rules) if remaining_rules else 0.0

    # OPT-060: Calculate domain-specific metrics (optimized version)
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

    # OPT-064: Average confidence
    avg_confidence = sum(approved_confidences) / len(approved_confidences) if approved_confidences else 0.0

    # OPT-065: Quality degradation detection (will be checked by caller)
    quality_floor_reached = avg_confidence < 0.75

    # OPT-049, OPT-070: Pass summary
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

    # OPT-057a: Return metrics dict
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
        print("\n⚠️ Quality floor reached (avg confidence <0.75)")
        return True
    return False


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Tag optimization with vocabulary-aware intelligence and HITL workflow (OPT-001)."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Optimize rule tags through Claude reasoning')
    parser.add_argument('--auto-approve', action='store_true',
                        help='Automatically approve high-confidence tags (OPT-011, OPT-056)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rules to process (OPT-002)')
    parser.add_argument('--state', type=str, default='needs_tags',
                        help='Filter rules by tags_state (default: needs_tags)')

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Get paths from config
    db_path = BASE_DIR / config['structure']['database_path']
    templates_dir = BASE_DIR / config['structure']['templates_dir']
    vocab_path = BASE_DIR / config.get('vocabulary_file', 'config/tag-vocabulary.yaml')

    # Load template (OPT-034)
    template_path = templates_dir / 'runtime-template-tag-optimization.txt'
    try:
        with open(template_path) as f:
            template = f.read()
    except Exception as e:
        print(f"Error loading template from {template_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Load vocabulary (OPT-019, OPT-035)
    vocab = load_vocabulary(vocab_path)

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # OPT-072, OPT-073: Check if any rules need optimization
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
    needs_tags_count = cursor.fetchone()[0]

    if needs_tags_count == 0:
        # OPT-072: No rules require optimization
        stats = get_database_statistics(db_path)
        print("\nNo rules require tag optimization.\n")
        print("Database state:")
        print(f"  Total rules: {stats['total']}")
        print(f"  Curated: {stats['curated']}")
        print(f"  Refined: {stats['refined']}")
        print(f"  Pending review: {stats['pending_review']}")
        print(f"  Needs tags: 0\n")

        if stats['total'] == 0:
            print("Database is empty. Run 'make chatlogs-extract' to import rules from chatlogs.")
        elif stats['curated'] + stats['refined'] > 0:
            print("All rules have been tagged. Use 'make tags-stats' to view tag distribution.")
        else:
            print("All pending rules require manual review. Run 'make tags-optimize' for interactive tagging.")

        conn.close()
        return 0

    # OPT-056: Determine if running in iterative mode
    iterative_mode = args.auto_approve and args.limit is None

    if iterative_mode:
        # OPT-057: Multi-pass iterative optimization
        print("Tag Optimization - Multi-Pass Iterative Mode")
        print("="*70)

        # Load tag optimization config (OPT-055, OPT-057)
        try:
            build_config_path = BASE_DIR.parent.parent / 'build' / 'config' / 'build-constants.yaml'
            with open(build_config_path) as f:
                build_config = yaml.safe_load(f)
            tag_opt_config = build_config.get('tag_optimization', {})
        except:
            # Fallback to defaults if build config not accessible
            tag_opt_config = {
                'parallel_max_workers': 3,
                'pass_1_threshold': 0.9,
                'pass_2_threshold': 0.9,
                'pass_3_threshold': 0.75,
                'convergence_max_passes': 5
            }

        max_passes = tag_opt_config.get('convergence_max_passes', 5)
        max_workers = tag_opt_config.get('parallel_max_workers', 3)

        # OPT-057b: Calculate corpus size
        cursor.execute("SELECT id FROM rules WHERE tags_state = 'needs_tags'")
        corpus_size = len(cursor.fetchall())

        # OPT-058: Calculate cost limit
        cost_limit = max(500, int(corpus_size * 0.5))

        print(f"\nCorpus size: {corpus_size} rules")
        print(f"Cost limit: {cost_limit} LLM calls")
        print(f"Max passes: {max_passes}")
        print(f"Parallel workers: {max_workers}")

        pass_number = 0
        total_llm_calls = 0
        prev_avg_confidence = None

        # OPT-057: Multi-pass loop
        while pass_number < max_passes:
            # Query remaining rules
            cursor.execute(f"SELECT * FROM rules WHERE tags_state = 'needs_tags'")
            remaining_rules = [dict(row) for row in cursor.fetchall()]

            if len(remaining_rules) == 0:
                print("\n✓ All rules tagged")
                break

            if total_llm_calls >= cost_limit:
                print(f"\n⚠ Cost limit reached ({total_llm_calls} >= {cost_limit} calls)")
                break

            # Determine confidence threshold for this pass (OPT-045)
            if pass_number == 0:
                confidence_threshold = tag_opt_config.get('pass_1_threshold', 0.9)
            elif pass_number == 1:
                confidence_threshold = tag_opt_config.get('pass_2_threshold', 0.9)
            else:
                confidence_threshold = tag_opt_config.get('pass_3_threshold', 0.75)

            print(f"\nPass {pass_number + 1} configuration:")
            print(f"  Confidence threshold: {confidence_threshold}")
            print(f"  Remaining rules: {len(remaining_rules)}")

            # Run optimization pass (OPT-057a)
            pass_results = run_optimization_pass(
                remaining_rules, pass_number, vocab_path, config,
                db_path, template, args.auto_approve, confidence_threshold, max_workers
            )

            # OPT-065: Quality degradation detection
            if pass_number > 0 and prev_avg_confidence is not None:
                confidence_drop = prev_avg_confidence - pass_results['avg_confidence']
                if confidence_drop > 0.15:
                    print(f"\n⚠ Warning: Confidence dropped by {confidence_drop:.2f} (from {prev_avg_confidence:.2f} to {pass_results['avg_confidence']:.2f})")

            prev_avg_confidence = pass_results['avg_confidence']

            total_llm_calls += len(remaining_rules)

            # OPT-067: Check convergence
            if should_stop_iteration(pass_results):
                break

            pass_number += 1

        # OPT-070: Multi-pass summary report
        print(f"\n{'='*70}")
        print("Multi-Pass Optimization Complete")
        print(f"{'='*70}")
        print(f"\nConvergence Details:")
        print(f"  Total passes: {pass_number + 1}")
        print(f"  Total LLM calls: {total_llm_calls}")

        # Final state
        cursor.execute("SELECT COUNT(*) FROM rules")
        total_rules = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state IN ('curated', 'refined')")
        tagged_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM rules WHERE tags_state = 'needs_tags'")
        remaining_count = cursor.fetchone()[0]

        print(f"\nFinal State:")
        print(f"  Total rules: {total_rules}")
        print(f"  Tagged: {tagged_count} ({tagged_count/total_rules*100:.1f}%)")
        print(f"  Needs review: {remaining_count} ({remaining_count/total_rules*100:.1f}%)")

        # Vocabulary growth
        all_tags = load_all_tier2_tags_from_vocabulary(vocab_path)
        unique_tags = len(set(all_tags))
        print(f"\nVocabulary Growth:")
        print(f"  Unique tags created: {unique_tags}")

        if remaining_count > 0:
            print(f"\nNext Steps:")
            print(f"  {remaining_count} rules require manual review")
            print(f"  Run 'make tags-optimize' for interactive tagging")

    else:
        # Single-pass mode (legacy behavior)
        print("Tag Optimization - Single Pass Mode")
        print("="*70)

        # Query rules (OPT-002)
        query = f"SELECT * FROM rules WHERE tags_state = ?"
        params = [args.state]

        if args.limit:
            query += " LIMIT ?"
            params.append(args.limit)

        cursor.execute(query, params)
        rules = [dict(row) for row in cursor.fetchall()]

        if len(rules) == 0:
            print(f"\nNo rules found with tags_state='{args.state}'")
            conn.close()
            return 0

        print(f"\nProcessing {len(rules)} rules...")

        # Default confidence threshold for single-pass (OPT-011)
        confidence_threshold = 0.70
        max_workers = 3  # Default for single-pass

        # Run single pass
        pass_results = run_optimization_pass(
            rules, 0, vocab_path, config,
            db_path, template, args.auto_approve, confidence_threshold, max_workers
        )

        print(f"\nOptimization complete:")
        print(f"  Approved: {pass_results['approved_count']}")
        print(f"  Remaining: {pass_results['remaining_count']}")

    conn.close()
    return 0


# ============================================================================
# ENTRY POINT
# ============================================================================


if __name__ == '__main__':
    sys.exit(main())
