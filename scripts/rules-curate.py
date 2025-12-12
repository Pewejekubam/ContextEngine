#!/usr/bin/env python3
"""
Automated rule curation with CI/CD JSON output

Implements constraints: CUR-001 through CUR-137
Generated from: build/modules/runtime-script-rules-curate.yaml v1.1.0
"""

import sys
import json
import sqlite3
import argparse
import subprocess
import random
import time
from pathlib import Path
from datetime import datetime, UTC
from collections import defaultdict

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml


# ============================================================================
# CUSTOM EXCEPTIONS (CUR-126, CUR-127, CUR-128)
# ============================================================================

class ValidationError(Exception):
    """Raised when LLM response fails schema validation."""
    pass


class RateLimitError(Exception):
    """Raised when Claude API returns 429."""
    pass


class LLMError(Exception):
    """Raised for general LLM invocation failures."""
    pass


# ============================================================================
# CONFIGURATION LOADING (CUR-001, CUR-031, CUR-062)
# ============================================================================

def load_curation_config(config_path):
    """Load curation configuration with defaults (CUR-001, CUR-031, CUR-062).

    Returns both curation settings and database path for connection.
    v1.3.0: Added auto_resolution config loading.
    """
    config_path = Path(config_path)
    with open(config_path) as f:
        config = yaml.safe_load(f)

    curation = config.get('curation', {})

    # Resolve database path relative to config file location
    # Config is in .context-engine/config/, database is in .context-engine/data/
    context_engine_home = config_path.parent.parent
    db_path = context_engine_home / config['structure']['database_path']
    templates_dir = context_engine_home / config['structure'].get('templates_dir', 'templates')

    # v1.3.0: Auto-resolution config (CUR-108, CUR-116, CUR-119, CUR-126)
    auto_res = curation.get('auto_resolution', {})

    return {
        'enabled': curation.get('enabled', True),
        'confidence_threshold': curation.get('confidence_threshold', 0.70),
        'domain_migrations': curation.get('domain_migrations', []),
        'conflict_resolution': curation.get('conflict_resolution', 'flag'),
        'archive_scopes': curation.get('archive_scopes', ['historical']),
        'database_path': db_path,
        'templates_dir': templates_dir,
        # v1.3.0: Auto-resolution settings
        'auto_resolution': {
            'enabled': auto_res.get('enabled', True),
            'confidence_threshold': auto_res.get('confidence_threshold', 0.80),
            'cost_limit': auto_res.get('cost_limit', 5.00),
            'max_conflicts_per_run': auto_res.get('max_conflicts_per_run', 50),
            'timeout_seconds': auto_res.get('timeout_seconds', 30)
        }
    }


# ============================================================================
# METADATA HELPERS
# ============================================================================

def load_rule_metadata(conn, rule_id):
    """Load rule metadata JSON."""
    cursor = conn.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,))
    row = cursor.fetchone()
    return json.loads(row['metadata'] or '{}') if row else {}


def save_rule_metadata(conn, rule_id, metadata):
    """Save rule metadata JSON."""
    conn.execute(
        "UPDATE rules SET metadata = ? WHERE id = ?",
        (json.dumps(metadata), rule_id)
    )


def load_rule_full(conn, rule_id):
    """Load complete rule content for LLM input (CUR-102)."""
    cursor = conn.execute("""
        SELECT id, type, title, description, domain, confidence, lifecycle, metadata
        FROM rules WHERE id = ?
    """, (rule_id,))
    row = cursor.fetchone()

    if not row:
        return None

    metadata = json.loads(row['metadata'] or '{}')

    return {
        'id': row['id'],
        'type': row['type'],
        'title': row['title'],
        'description': row['description'] or '',
        'domain': row['domain'],
        'confidence': row['confidence'],
        'lifecycle': row['lifecycle'],
        'relationships': metadata.get('relationships', [])
    }


# ============================================================================
# EXACT DUPLICATE DETECTION (CUR-010 through CUR-015)
# ============================================================================

def find_exact_duplicates(conn):
    """Find rules with identical (type, domain, title) tuples (CUR-010)."""
    cursor = conn.execute("""
        SELECT type, domain, title, GROUP_CONCAT(id) as rule_ids, COUNT(*) as cnt
        FROM rules
        WHERE lifecycle = 'active'
        GROUP BY type, domain, title
        HAVING cnt > 1
    """)
    return cursor.fetchall()


def merge_duplicates(conn, duplicate_group):
    """Merge duplicate rules, keeping lowest ID as canonical (CUR-012 through CUR-015)."""
    rule_ids = sorted([int(rid) for rid in duplicate_group['rule_ids'].split(',')])
    canonical_id = rule_ids[0]
    duplicates_to_delete = rule_ids[1:]

    # Update canonical rule metadata
    metadata = load_rule_metadata(conn, canonical_id)
    metadata['merged_from'] = [str(rid) for rid in duplicates_to_delete]
    metadata['merged_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
    save_rule_metadata(conn, canonical_id, metadata)

    # Delete duplicates (CUR-013)
    conn.executemany(
        "DELETE FROM rules WHERE id = ?",
        [(str(rid),) for rid in duplicates_to_delete]
    )

    return {
        'action': 'merge',
        'kept': str(canonical_id),
        'deleted': [str(rid) for rid in duplicates_to_delete]
    }


def process_all_duplicates(conn):
    """Find and merge all exact duplicate groups."""
    changes = []
    for duplicate_group in find_exact_duplicates(conn):
        change = merge_duplicates(conn, duplicate_group)
        changes.append(change)
    return changes


# ============================================================================
# SUPERSESSION ENFORCEMENT (CUR-020 through CUR-024)
# ============================================================================

def enforce_supersession(conn):
    """Set lifecycle='superseded' for rules with superseded_by relationships (CUR-020 through CUR-024)."""
    changes = []

    # CUR-021: Check rule_relationships table
    cursor = conn.execute("""
        SELECT DISTINCT r.id
        FROM rules r
        JOIN rule_relationships rr ON r.id = rr.from_rule
        WHERE rr.relationship_type = 'superseded_by'
        AND r.lifecycle = 'active'
    """)
    from_table = [row['id'] for row in cursor.fetchall()]

    # CUR-022: Check metadata.relationships JSON
    cursor = conn.execute("""
        SELECT id, metadata FROM rules
        WHERE lifecycle = 'active'
        AND metadata IS NOT NULL
        AND json_extract(metadata, '$.relationships') IS NOT NULL
    """)
    from_metadata = []
    for row in cursor.fetchall():
        metadata = json.loads(row['metadata'])
        relationships = metadata.get('relationships', [])
        if any(r.get('type') == 'superseded_by' for r in relationships):
            from_metadata.append(row['id'])

    # CUR-023: Union and update
    to_supersede = set(from_table) | set(from_metadata)
    for rule_id in to_supersede:
        conn.execute(
            "UPDATE rules SET lifecycle = 'superseded' WHERE id = ?",
            (rule_id,)
        )
        changes.append({'action': 'supersede', 'rule': rule_id})

    return changes


# ============================================================================
# CONFIDENCE THRESHOLD ENFORCEMENT (CUR-030 through CUR-035)
# ============================================================================

def archive_low_confidence(conn, threshold):
    """Archive rules below confidence threshold (CUR-030 through CUR-035)."""
    cursor = conn.execute("""
        SELECT id, metadata FROM rules
        WHERE lifecycle = 'active'
        AND confidence IS NOT NULL
        AND confidence < ?
    """, (threshold,))

    changes = []
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    for row in cursor.fetchall():
        metadata = json.loads(row['metadata'] or '{}')
        metadata['archive_reason'] = 'below_confidence_threshold'
        metadata['archived_at'] = now
        metadata['threshold_applied'] = threshold

        conn.execute("""
            UPDATE rules
            SET lifecycle = 'archived', metadata = ?
            WHERE id = ?
        """, (json.dumps(metadata), row['id']))

        changes.append({'action': 'archive', 'rule': row['id'], 'reason': 'low_confidence'})

    return changes


# ============================================================================
# DOMAIN MIGRATION (CUR-040 through CUR-045)
# ============================================================================

def apply_domain_migrations(conn, migrations):
    """Apply domain renames from configuration (CUR-040 through CUR-045)."""
    changes = []
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    for migration in migrations:
        from_domain = migration['from']
        to_domain = migration['to']
        effective_date = migration.get('effective_date')

        # Build query
        query = "SELECT id, domain, metadata, created_at FROM rules WHERE domain = ?"
        params = [from_domain]

        # CUR-042: Optional date filter
        if effective_date:
            query += " AND created_at < ?"
            params.append(effective_date)

        cursor = conn.execute(query, params)

        for row in cursor.fetchall():
            metadata = json.loads(row['metadata'] or '{}')

            # CUR-044, CUR-045: Track domain history
            history = metadata.get('domain_history', [])
            history.append({
                'from': from_domain,
                'to': to_domain,
                'migrated_at': now
            })
            metadata['domain_history'] = history

            conn.execute("""
                UPDATE rules SET domain = ?, metadata = ? WHERE id = ?
            """, (to_domain, json.dumps(metadata), row['id']))

            changes.append({
                'action': 'domain_migrate',
                'rule': row['id'],
                'from': from_domain,
                'to': to_domain
            })

    return changes


# ============================================================================
# SCOPE-BASED ARCHIVAL (CUR-060 through CUR-063)
# ============================================================================

def archive_excluded_scopes(conn, archive_scopes):
    """Archive rules with reusability_scope in excluded list (CUR-060 through CUR-063)."""
    changes = []
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    cursor = conn.execute("""
        SELECT id, metadata FROM rules
        WHERE lifecycle = 'active'
        AND metadata IS NOT NULL
    """)

    for row in cursor.fetchall():
        metadata = json.loads(row['metadata'] or '{}')
        scope = metadata.get('reusability_scope')

        # CUR-060: Check if scope in excluded list
        if scope in archive_scopes:
            # CUR-063: Set archive reason
            metadata['archive_reason'] = 'scope_excluded'
            metadata['archived_at'] = now

            conn.execute("""
                UPDATE rules SET lifecycle = 'archived', metadata = ?
                WHERE id = ?
            """, (json.dumps(metadata), row['id']))

            changes.append({
                'action': 'archive',
                'rule': row['id'],
                'reason': 'scope_excluded',
                'scope': scope
            })

    return changes


# ============================================================================
# CIRCULAR CONFLICT DETECTION (CUR-121 through CUR-125)
# ============================================================================

def merge_overlapping_sets(sets):
    """Merge sets that share any elements."""
    if not sets:
        return []

    merged = []
    for s in sets:
        found = False
        for i, m in enumerate(merged):
            if s & m:  # Intersection exists
                merged[i] = m | s
                found = True
                break
        if not found:
            merged.append(s)

    # Repeat until stable
    prev_len = 0
    while len(merged) != prev_len:
        prev_len = len(merged)
        new_merged = []
        for s in merged:
            found = False
            for i, m in enumerate(new_merged):
                if s & m:
                    new_merged[i] = m | s
                    found = True
                    break
            if not found:
                new_merged.append(s)
        merged = new_merged

    return merged


def detect_circular_conflicts(conflicts):
    """Detect circular conflict chains using DFS (CUR-121 through CUR-125).

    Args:
        conflicts: List of (rule_a, rule_b) tuples

    Returns:
        tuple: (non_circular_conflicts, circular_groups)
        - non_circular_conflicts: List of conflict pairs safe to process
        - circular_groups: List of rule ID sets that form cycles
    """
    # Build adjacency list
    graph = defaultdict(set)
    for rule_a, rule_b in conflicts:
        graph[rule_a].add(rule_b)
        graph[rule_b].add(rule_a)

    visited = set()
    in_stack = set()
    cycles = []

    def dfs(node, parent, path):
        """DFS to detect cycles."""
        visited.add(node)
        in_stack.add(node)
        path.append(node)

        for neighbor in graph[node]:
            if neighbor == parent:
                continue
            if neighbor in in_stack:
                # Found cycle - extract it
                cycle_start = path.index(neighbor)
                cycle = set(path[cycle_start:])
                cycles.append(cycle)
            elif neighbor not in visited:
                dfs(neighbor, node, path)

        path.pop()
        in_stack.remove(node)

    # Run DFS from each unvisited node
    for node in graph:
        if node not in visited:
            dfs(node, None, [])

    # Merge overlapping cycles
    merged_cycles = merge_overlapping_sets(cycles)

    # Identify rules in cycles
    rules_in_cycles = set()
    for cycle in merged_cycles:
        rules_in_cycles.update(cycle)

    # Split conflicts
    non_circular = [
        (a, b) for a, b in conflicts
        if a not in rules_in_cycles and b not in rules_in_cycles
    ]

    return non_circular, merged_cycles


# ============================================================================
# LLM INVOCATION (CUR-100, CUR-126, CUR-128)
# ============================================================================

def load_template(templates_dir, template_name):
    """Load prompt template from templates directory."""
    template_path = templates_dir / f"{template_name}.txt"
    with open(template_path) as f:
        return f.read()


def invoke_claude_with_retry(prompt, timeout, max_retries=3):
    """Invoke Claude CLI with exponential backoff (CUR-128).

    Backoff algorithm:
    - Initial delay: 2 seconds
    - Multiplier: 2x per retry
    - Delay cap: 60 seconds
    - Jitter: +/-25% randomization
    """
    delay = 2  # Initial delay

    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                ['claude', '--print', '-p', prompt],
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                return result.stdout.strip()

            # Check for rate limit in stderr
            if '429' in result.stderr or 'rate limit' in result.stderr.lower():
                raise RateLimitError(result.stderr)

            # Other error
            raise LLMError(result.stderr)

        except subprocess.TimeoutExpired:
            raise TimeoutError(f"Claude call timed out after {timeout}s")

        except RateLimitError:
            if attempt == max_retries - 1:
                raise  # Final attempt failed

            # Exponential backoff with jitter (CUR-128)
            jitter = random.uniform(0.75, 1.25)
            sleep_time = min(delay * jitter, 60)
            time.sleep(sleep_time)
            delay *= 2

    raise LLMError("Max retries exceeded")


# ============================================================================
# VERDICT VALIDATION (CUR-104, CUR-105)
# ============================================================================

def validate_verdict_schema(result):
    """Validate LLM response matches expected schema (CUR-104, CUR-105)."""
    required_keys = ['verdict', 'confidence', 'reasoning']
    for key in required_keys:
        if key not in result:
            raise ValidationError(f"Missing required key: {key}")

    valid_verdicts = ['supersede', 'merge', 'coexist', 'escalate']
    if result['verdict'] not in valid_verdicts:
        raise ValidationError(f"Invalid verdict: {result['verdict']}")

    if not isinstance(result['confidence'], (int, float)):
        raise ValidationError(f"Confidence must be numeric: {result['confidence']}")

    if not 0.0 <= result['confidence'] <= 1.0:
        raise ValidationError(f"Confidence must be 0.0-1.0: {result['confidence']}")

    if result['verdict'] in ('supersede',) and result.get('keep') not in ('rule_a', 'rule_b'):
        raise ValidationError(f"Verdict {result['verdict']} requires keep='rule_a' or 'rule_b'")


# ============================================================================
# CONFLICT ESCALATION (CUR-131, CUR-137)
# ============================================================================

def escalate_conflict(conn, rule_a_id, rule_b_id, now, confidence, reasoning, source):
    """Escalate conflict for manual review (CUR-131, CUR-137)."""

    # Update rule_a metadata
    metadata_a = load_rule_metadata(conn, rule_a_id)
    escalation_history = metadata_a.get('escalation_history', [])
    escalation_history.append({
        'escalated_at': now,
        'counterpart': rule_b_id,
        'confidence': confidence,
        'reasoning': reasoning,
        'escalation_source': source
    })
    metadata_a['escalation_history'] = escalation_history
    save_rule_metadata(conn, rule_a_id, metadata_a)

    # Update rule_b metadata
    metadata_b = load_rule_metadata(conn, rule_b_id)
    escalation_history = metadata_b.get('escalation_history', [])
    escalation_history.append({
        'escalated_at': now,
        'counterpart': rule_a_id,
        'confidence': confidence,
        'reasoning': reasoning,
        'escalation_source': source
    })
    metadata_b['escalation_history'] = escalation_history
    save_rule_metadata(conn, rule_b_id, metadata_b)

    return {
        'action': 'conflict_escalated',
        'rules': [rule_a_id, rule_b_id],
        'confidence': confidence,
        'reasoning': reasoning,
        'escalation_source': source
    }


# ============================================================================
# RELATIONSHIP MANAGEMENT
# ============================================================================

def remove_conflict_relationship(conn, rule_a_id, rule_b_id):
    """Remove conflicts_with relationship between two rules."""
    conn.execute("""
        DELETE FROM rule_relationships
        WHERE relationship_type = 'conflicts_with'
        AND ((from_rule = ? AND to_rule = ?) OR (from_rule = ? AND to_rule = ?))
    """, (rule_a_id, rule_b_id, rule_b_id, rule_a_id))


# ============================================================================
# VERDICT APPLICATION (CUR-133 through CUR-137)
# ============================================================================

def apply_supersede(conn, rule_a_id, rule_b_id, keep, confidence, reasoning, now):
    """Apply supersession verdict (CUR-133, CUR-134)."""

    if keep == 'rule_a':
        survivor_id, superseded_id = rule_a_id, rule_b_id
    else:
        survivor_id, superseded_id = rule_b_id, rule_a_id

    # Update superseded rule
    conn.execute(
        "UPDATE rules SET lifecycle = 'superseded' WHERE id = ?",
        (superseded_id,)
    )

    # Update metadata
    metadata = load_rule_metadata(conn, superseded_id)
    metadata['superseded_by'] = survivor_id
    resolution_history = metadata.get('conflict_resolution_history', [])
    resolution_history.append({
        'resolved_at': now,
        'verdict': 'supersede',
        'counterpart': survivor_id,
        'confidence': confidence,
        'reasoning': reasoning,
        'method': 'llm_assisted'
    })
    metadata['conflict_resolution_history'] = resolution_history
    save_rule_metadata(conn, superseded_id, metadata)

    # Add supersession relationship
    conn.execute("""
        INSERT OR REPLACE INTO rule_relationships (from_rule, to_rule, relationship_type)
        VALUES (?, ?, 'superseded_by')
    """, (superseded_id, survivor_id))

    # Remove conflicts_with relationship
    remove_conflict_relationship(conn, rule_a_id, rule_b_id)

    return {
        'action': 'conflict_resolved',
        'verdict': 'supersede',
        'kept': survivor_id,
        'superseded': superseded_id,
        'confidence': confidence,
        'reasoning': reasoning
    }


def apply_merge(conn, rule_a_id, rule_b_id, confidence, reasoning, now):
    """Apply merge verdict (CUR-135)."""

    # Keep lower ID as canonical
    if rule_a_id < rule_b_id:
        canonical_id, archived_id = rule_a_id, rule_b_id
    else:
        canonical_id, archived_id = rule_b_id, rule_a_id

    # Archive non-canonical rule
    conn.execute(
        "UPDATE rules SET lifecycle = 'archived' WHERE id = ?",
        (archived_id,)
    )

    # Update canonical metadata
    canonical_metadata = load_rule_metadata(conn, canonical_id)
    merged_from = canonical_metadata.get('merged_from', [])
    if archived_id not in merged_from:
        merged_from.append(archived_id)
    canonical_metadata['merged_from'] = merged_from
    canonical_metadata['merged_at'] = now
    save_rule_metadata(conn, canonical_id, canonical_metadata)

    # Update archived metadata
    archived_metadata = load_rule_metadata(conn, archived_id)
    archived_metadata['archive_reason'] = 'conflict_merge'
    archived_metadata['merged_into'] = canonical_id
    resolution_history = archived_metadata.get('conflict_resolution_history', [])
    resolution_history.append({
        'resolved_at': now,
        'verdict': 'merge',
        'counterpart': canonical_id,
        'confidence': confidence,
        'reasoning': reasoning,
        'method': 'llm_assisted'
    })
    archived_metadata['conflict_resolution_history'] = resolution_history
    save_rule_metadata(conn, archived_id, archived_metadata)

    # Remove conflicts_with relationship
    remove_conflict_relationship(conn, rule_a_id, rule_b_id)

    return {
        'action': 'conflict_resolved',
        'verdict': 'merge',
        'kept': canonical_id,
        'archived': archived_id,
        'confidence': confidence,
        'reasoning': reasoning
    }


def apply_coexist(conn, rule_a_id, rule_b_id, confidence, reasoning, now):
    """Apply coexist verdict - remove false positive conflict (CUR-136)."""

    # Update both rules with false positive marker
    for rule_id, counterpart_id in [(rule_a_id, rule_b_id), (rule_b_id, rule_a_id)]:
        metadata = load_rule_metadata(conn, rule_id)
        false_positives = metadata.get('false_positive_conflicts', [])
        false_positives.append({
            'counterpart': counterpart_id,
            'resolved_at': now,
            'confidence': confidence,
            'reasoning': reasoning
        })
        metadata['false_positive_conflicts'] = false_positives

        # Remove from metadata.relationships if present
        relationships = metadata.get('relationships', [])
        metadata['relationships'] = [
            r for r in relationships
            if not (r.get('type') == 'conflicts_with' and r.get('target') == counterpart_id)
        ]

        save_rule_metadata(conn, rule_id, metadata)

    # Remove from rule_relationships table
    remove_conflict_relationship(conn, rule_a_id, rule_b_id)

    return {
        'action': 'conflict_resolved',
        'verdict': 'coexist',
        'rules': [rule_a_id, rule_b_id],
        'confidence': confidence,
        'reasoning': reasoning
    }


# ============================================================================
# LLM CONFLICT RESOLUTION (CUR-100 through CUR-137)
# ============================================================================

def resolve_conflict_llm(conn, rule_a_id, rule_b_id, config, templates_dir, verbose):
    """Resolve conflict using LLM reasoning (CUR-100 through CUR-137).

    Args:
        conn: Database connection
        rule_a_id: First rule in conflict pair
        rule_b_id: Second rule in conflict pair
        config: Auto-resolution configuration
        templates_dir: Path to templates directory
        verbose: Enable verbose logging

    Returns:
        dict: Resolution result with action, verdict, confidence, etc.
    """
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    # CUR-102: Load full rule content
    rule_a = load_rule_full(conn, rule_a_id)
    rule_b = load_rule_full(conn, rule_b_id)

    if not rule_a or not rule_b:
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=0.0,
            reasoning="One or both rules not found in database",
            source='llm_error'
        )

    # CUR-110: Check for axiom conflicts (always escalate)
    if rule_a['type'].startswith('AX-') or rule_b['type'].startswith('AX-'):
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=0.0,
            reasoning="Conflict involves system axiom - requires human review",
            source='axiom_conflict'
        )

    # CUR-102: Format rules for LLM input
    rule_a_formatted = json.dumps({
        'id': rule_a['id'],
        'type': rule_a['type'],
        'title': rule_a['title'],
        'description': rule_a['description'],
        'domain': rule_a['domain'],
        'confidence': rule_a['confidence'],
        'lifecycle': rule_a['lifecycle'],
        'relationships': rule_a.get('relationships', [])
    }, indent=2)

    rule_b_formatted = json.dumps({
        'id': rule_b['id'],
        'type': rule_b['type'],
        'title': rule_b['title'],
        'description': rule_b['description'],
        'domain': rule_b['domain'],
        'confidence': rule_b['confidence'],
        'lifecycle': rule_b['lifecycle'],
        'relationships': rule_b.get('relationships', [])
    }, indent=2)

    # Load and populate template
    template = load_template(templates_dir, 'runtime-template-rules-conflict-resolution')
    prompt = template.replace('{rule_a_formatted}', rule_a_formatted)
    prompt = prompt.replace('{rule_b_formatted}', rule_b_formatted)

    # CUR-126, CUR-128: Invoke LLM with timeout and retry
    try:
        response = invoke_claude_with_retry(
            prompt,
            timeout=config['timeout_seconds'],
            max_retries=3
        )
    except TimeoutError:
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=0.0,
            reasoning=f"LLM timeout after {config['timeout_seconds']} seconds",
            source='timeout'
        )
    except RateLimitError as e:
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=0.0,
            reasoning=f"Rate limit exceeded after retries: {e}",
            source='llm_error'
        )
    except (LLMError, Exception) as e:
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=0.0,
            reasoning=f"LLM invocation failed: {e}",
            source='llm_error'
        )

    # CUR-104, CUR-127: Parse and validate response
    try:
        # Extract JSON from response (may have markdown code fences)
        response_text = response.strip()
        if response_text.startswith('```'):
            # Remove code fences
            lines = response_text.split('\n')
            json_lines = [l for l in lines if not l.startswith('```')]
            response_text = '\n'.join(json_lines)

        result = json.loads(response_text)
        validate_verdict_schema(result)
    except (json.JSONDecodeError, ValidationError) as e:
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=0.0,
            reasoning=f"Invalid LLM response: {e}",
            source='llm_error'
        )

    verdict = result['verdict']
    keep = result.get('keep')
    confidence = result['confidence']
    reasoning = result['reasoning']

    if verbose:
        log_verbose(f"[LLM] {rule_a_id} vs {rule_b_id}: verdict={verdict}, confidence={confidence}", verbose)

    # CUR-108, CUR-111: Check confidence threshold
    if confidence < config['confidence_threshold']:
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=confidence,
            reasoning=reasoning,
            source='below_threshold'
        )

    # CUR-133-137: Apply verdict
    if verdict == 'supersede':
        return apply_supersede(conn, rule_a_id, rule_b_id, keep, confidence, reasoning, now)
    elif verdict == 'merge':
        return apply_merge(conn, rule_a_id, rule_b_id, confidence, reasoning, now)
    elif verdict == 'coexist':
        return apply_coexist(conn, rule_a_id, rule_b_id, confidence, reasoning, now)
    else:  # escalate
        return escalate_conflict(
            conn, rule_a_id, rule_b_id, now,
            confidence=confidence,
            reasoning=reasoning,
            source='llm_escalate'
        )


# ============================================================================
# CONFLICT DETECTION AND PROCESSING (CUR-050 through CUR-057, CUR-100+)
# ============================================================================

def detect_conflicts_deterministic(conn, all_conflicts, resolution_strategy, now, verbose):
    """Process conflicts using deterministic strategies (CUR-054 through CUR-057)."""
    changes = []

    for rule_a, rule_b in all_conflicts:
        # CUR-054: Default strategy is 'flag'
        if resolution_strategy == 'flag':
            changes.append({
                'action': 'conflict_flagged',
                'rules': [rule_a, rule_b],
                'resolution': 'manual_required'
            })

        elif resolution_strategy == 'keep_newer':
            # CUR-055: Archive rule with earlier created_at
            cursor = conn.execute("""
                SELECT id, created_at, metadata FROM rules
                WHERE id IN (?, ?)
                ORDER BY created_at DESC
            """, (rule_a, rule_b))
            rows = cursor.fetchall()
            newer, older = rows[0], rows[1]

            # Archive older rule
            metadata = json.loads(older['metadata'] or '{}')
            metadata['archive_reason'] = 'conflict_resolved'
            metadata['conflict_resolution'] = {
                'strategy': 'keep_newer',
                'kept_rule': newer['id'],
                'resolved_at': now
            }
            conn.execute("""
                UPDATE rules SET lifecycle = 'archived', metadata = ?
                WHERE id = ?
            """, (json.dumps(metadata), older['id']))

            changes.append({
                'action': 'conflict_resolved',
                'kept': newer['id'],
                'archived': older['id'],
                'strategy': 'keep_newer'
            })

        elif resolution_strategy == 'keep_higher_confidence':
            # CUR-056: Archive rule with lower confidence
            cursor = conn.execute("""
                SELECT id, confidence, metadata FROM rules
                WHERE id IN (?, ?)
                ORDER BY confidence DESC NULLS LAST
            """, (rule_a, rule_b))
            rows = cursor.fetchall()
            higher, lower = rows[0], rows[1]

            # Archive lower confidence rule
            metadata = json.loads(lower['metadata'] or '{}')
            metadata['archive_reason'] = 'conflict_resolved'
            metadata['conflict_resolution'] = {
                'strategy': 'keep_higher_confidence',
                'kept_rule': higher['id'],
                'resolved_at': now
            }
            conn.execute("""
                UPDATE rules SET lifecycle = 'archived', metadata = ?
                WHERE id = ?
            """, (json.dumps(metadata), lower['id']))

            changes.append({
                'action': 'conflict_resolved',
                'kept': higher['id'],
                'archived': lower['id'],
                'strategy': 'keep_higher_confidence'
            })

    return changes


def detect_conflicts_llm(conn, all_conflicts, config, templates_dir, now, verbose):
    """Process conflicts using LLM-assisted resolution (CUR-100+)."""
    changes = []
    auto_config = config.get('auto_resolution', {})

    if not auto_config.get('enabled', True):
        if verbose:
            log_verbose("[Conflicts] LLM resolution disabled, flagging all conflicts", verbose)
        return detect_conflicts_deterministic(conn, all_conflicts, 'flag', now, verbose), 0.0

    confidence_threshold = auto_config.get('confidence_threshold', 0.80)
    cost_limit = auto_config.get('cost_limit', 5.00)
    max_conflicts = auto_config.get('max_conflicts_per_run', 50)

    # CUR-121-125: Detect circular conflicts
    non_circular, circular_groups = detect_circular_conflicts(all_conflicts)

    # Escalate circular conflicts
    for cycle in circular_groups:
        cycle_list = list(cycle)
        if verbose:
            log_verbose(f"[Conflicts] Escalating circular conflict group: {cycle_list}", verbose)

        for rule_id in cycle_list:
            metadata = load_rule_metadata(conn, rule_id)
            escalation_history = metadata.get('escalation_history', [])
            escalation_history.append({
                'escalated_at': now,
                'cycle_members': cycle_list,
                'reasoning': f"Part of circular conflict chain involving {len(cycle_list)} rules",
                'escalation_source': 'circular_conflict'
            })
            metadata['escalation_history'] = escalation_history
            save_rule_metadata(conn, rule_id, metadata)

        changes.append({
            'action': 'conflict_escalated',
            'rules': cycle_list,
            'escalation_source': 'circular_conflict',
            'reasoning': f"Circular conflict chain detected"
        })

    # Process non-circular conflicts
    estimated_cost = 0.0
    cost_per_conflict = 0.03  # CUR-117: Conservative estimate
    conflicts_processed = 0

    for rule_a, rule_b in non_circular:
        # CUR-119: Check max conflicts
        if conflicts_processed >= max_conflicts:
            if verbose:
                log_verbose(f"[Conflicts] Max conflicts reached ({max_conflicts}), flagging remainder", verbose)
            changes.append({
                'action': 'conflict_flagged',
                'rules': [rule_a, rule_b],
                'resolution': 'max_conflicts_reached'
            })
            continue

        # CUR-116-118: Check cost limit
        if estimated_cost + cost_per_conflict > cost_limit:
            if verbose:
                log_verbose(f"[Conflicts] Cost limit reached (${cost_limit:.2f}), flagging remainder", verbose)
            changes.append({
                'action': 'conflict_flagged',
                'rules': [rule_a, rule_b],
                'resolution': 'cost_limit_reached'
            })
            continue

        if verbose:
            log_verbose(f"[Conflicts] Processing conflict: {rule_a} vs {rule_b}", verbose)

        # CUR-100: Invoke LLM resolution
        result = resolve_conflict_llm(
            conn, rule_a, rule_b,
            {
                'confidence_threshold': confidence_threshold,
                'timeout_seconds': auto_config.get('timeout_seconds', 30)
            },
            templates_dir,
            verbose
        )

        changes.append(result)
        estimated_cost += cost_per_conflict
        conflicts_processed += 1

        if verbose:
            log_verbose(f"[Conflicts] Result: {result['action']} (confidence: {result.get('confidence', 'N/A')})", verbose)

    return changes, estimated_cost


def detect_conflicts(conn, resolution_strategy, config, now, verbose):
    """Detect and resolve rule conflicts (CUR-050 through CUR-057, CUR-100+).

    v1.3.0: Updated to support llm_assisted strategy and return (changes, cost) tuple.

    Args:
        conn: Database connection
        resolution_strategy: One of 'flag', 'keep_newer', 'keep_higher_confidence', 'llm_assisted'
        config: Full curation configuration
        now: ISO timestamp string
        verbose: Enable verbose logging

    Returns:
        tuple: (changes list, estimated_llm_cost float)
    """
    # CUR-050: Find conflicts from rule_relationships table
    cursor = conn.execute("""
        SELECT DISTINCT rr.from_rule, rr.to_rule
        FROM rule_relationships rr
        JOIN rules r1 ON rr.from_rule = r1.id
        JOIN rules r2 ON rr.to_rule = r2.id
        WHERE rr.relationship_type = 'conflicts_with'
        AND r1.lifecycle = 'active'
        AND r2.lifecycle = 'active'
    """)
    conflicts_from_table = [(row['from_rule'], row['to_rule']) for row in cursor.fetchall()]

    # CUR-051: Also check metadata.relationships
    cursor = conn.execute("""
        SELECT id, metadata FROM rules
        WHERE lifecycle = 'active'
        AND metadata IS NOT NULL
        AND json_extract(metadata, '$.relationships') IS NOT NULL
    """)
    conflicts_from_metadata = []
    for row in cursor.fetchall():
        metadata = json.loads(row['metadata'])
        relationships = metadata.get('relationships', [])
        for rel in relationships:
            if rel.get('type') == 'conflicts_with':
                target = rel.get('target')
                if target:
                    conflicts_from_metadata.append((row['id'], target))

    # Union all conflicts
    all_conflicts = list(set(conflicts_from_table) | set(conflicts_from_metadata))

    if verbose:
        log_verbose(f"[Conflicts] Found {len(all_conflicts)} total conflicts", verbose)

    # v1.3.0: Route to appropriate handler
    if resolution_strategy == 'llm_assisted':
        return detect_conflicts_llm(conn, all_conflicts, config, config.get('templates_dir'), now, verbose)
    else:
        changes = detect_conflicts_deterministic(conn, all_conflicts, resolution_strategy, now, verbose)
        return changes, 0.0  # No LLM cost for deterministic strategies


# ============================================================================
# OUTPUT FORMATTING (CUR-070 through CUR-074)
# ============================================================================

def build_result(changes, mode, config, now, estimated_llm_cost=0.0):
    """Build JSON result object from changes list (CUR-070 through CUR-074, v1.3.0)."""
    # Count actions by type
    stats = {
        'rules_processed': 0,
        'duplicates_merged': 0,
        'supersessions_enforced': 0,
        'archived_low_confidence': 0,
        'domains_migrated': 0,
        'conflicts_detected': 0,
        'conflicts_auto_resolved': 0,
        'conflicts_escalated': 0,
        'scopes_archived': 0,
        'estimated_llm_cost': estimated_llm_cost
    }

    for change in changes:
        action = change.get('action')
        if action == 'merge':
            stats['duplicates_merged'] += 1
        elif action == 'supersede':
            stats['supersessions_enforced'] += 1
        elif action == 'archive':
            reason = change.get('reason')
            if reason == 'low_confidence':
                stats['archived_low_confidence'] += 1
            elif reason == 'scope_excluded':
                stats['scopes_archived'] += 1
        elif action == 'domain_migrate':
            stats['domains_migrated'] += 1
        elif action == 'conflict_flagged':
            stats['conflicts_detected'] += 1
        elif action == 'conflict_resolved':
            stats['conflicts_detected'] += 1
            stats['conflicts_auto_resolved'] += 1
        elif action == 'conflict_escalated':
            stats['conflicts_detected'] += 1
            stats['conflicts_escalated'] += 1

    return {
        'run_id': now,
        'mode': mode,
        'config': {
            'confidence_threshold': config['confidence_threshold'],
            'conflict_resolution': config['conflict_resolution'],
            'domain_migrations_count': len(config['domain_migrations']),
            'archive_scopes': config['archive_scopes'],
            'auto_resolution': config.get('auto_resolution', {})
        },
        'stats': stats,
        'changes': changes,
        'warnings': [],
        'errors': [],
        'exit_code': 0
    }


def output_result(result):
    """Output result as JSON to stdout (CUR-070)."""
    print(json.dumps(result, indent=2))


# ============================================================================
# CLI AND MAIN EXECUTION
# ============================================================================

def log_verbose(msg, verbose=True):
    """Log message to stderr if verbose mode enabled."""
    if verbose:
        print(msg, file=sys.stderr)


def parse_args():
    """Parse command-line arguments (CUR-080 through CUR-083)."""
    parser = argparse.ArgumentParser(
        description='Automated rule curation with CI/CD JSON output'
    )
    parser.add_argument(
        '--mode',
        choices=['apply', 'dry-run'],
        default='apply',
        help='Execution mode: apply changes or dry-run preview (CUR-080, CUR-081)'
    )
    parser.add_argument(
        '--config',
        default=None,
        help='Path to deployment.yaml (CUR-082)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging to stderr (CUR-083)'
    )
    return parser.parse_args()


def main():
    """Automated rule curation for CI/CD pipeline execution."""
    args = parse_args()

    # Determine config path
    if args.config:
        config_path = Path(args.config)
    else:
        # Default: config/deployment.yaml relative to script
        script_dir = Path(__file__).parent
        config_path = script_dir.parent / 'config' / 'deployment.yaml'

    try:
        config = load_curation_config(config_path)
    except Exception as e:
        output_result({'error': f"Configuration error: {e}", 'exit_code': 2})
        return 2

    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    if not config['enabled']:
        output_result({
            'run_id': now,
            'mode': args.mode,
            'stats': {},
            'changes': [],
            'message': 'curation disabled',
            'exit_code': 0
        })
        return 0

    # Connect using database_path from config
    conn = sqlite3.connect(str(config['database_path']))
    conn.row_factory = sqlite3.Row

    try:
        changes = []
        estimated_llm_cost = 0.0

        if args.verbose:
            log_verbose(f"[Curation] Starting curation run (mode={args.mode})", args.verbose)

        # CUR-091: Fixed execution order
        if args.verbose:
            log_verbose("[Curation] Phase 1: Enforce supersession", args.verbose)
        changes.extend(enforce_supersession(conn))

        if args.verbose:
            log_verbose("[Curation] Phase 2: Merge duplicates", args.verbose)
        changes.extend(process_all_duplicates(conn))

        if args.verbose:
            log_verbose("[Curation] Phase 3: Archive low confidence", args.verbose)
        changes.extend(archive_low_confidence(conn, config['confidence_threshold']))

        if args.verbose:
            log_verbose("[Curation] Phase 4: Apply domain migrations", args.verbose)
        changes.extend(apply_domain_migrations(conn, config['domain_migrations']))

        if args.verbose:
            log_verbose("[Curation] Phase 5: Archive excluded scopes", args.verbose)
        changes.extend(archive_excluded_scopes(conn, config['archive_scopes']))

        # v1.3.0: Handle llm_assisted strategy (CUR-093)
        if args.verbose:
            log_verbose("[Curation] Phase 6: Detect and resolve conflicts", args.verbose)
        conflict_changes, estimated_llm_cost = detect_conflicts(
            conn,
            config['conflict_resolution'],
            config,
            now,
            args.verbose
        )
        changes.extend(conflict_changes)

        if args.mode == 'apply':
            conn.commit()
            if args.verbose:
                log_verbose("[Curation] Changes committed", args.verbose)
        else:
            conn.rollback()
            if args.verbose:
                log_verbose("[Curation] Dry-run mode: changes rolled back", args.verbose)

        output_result(build_result(changes, args.mode, config, now, estimated_llm_cost))
        return 0

    except Exception as e:
        conn.rollback()
        if args.verbose:
            log_verbose(f"[Curation] Error: {e}", args.verbose)
        output_result({'error': str(e), 'exit_code': 1})
        return 1

    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
