#!/usr/bin/env python3
"""
Automated rule curation with CI/CD JSON output

Implements constraints: CUR-001 through CUR-092
Generated from: specs/modules/runtime-script-rules-curate-v1.0.0.yaml
"""

import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, UTC
import re

# INV-023: Check Python version
if sys.version_info < (3, 8):
    print("Error: Python 3.8+ required", file=sys.stderr)
    sys.exit(1)

import yaml


# ============================================================================
# CONFIGURATION LOADING (CUR-001, CUR-031, CUR-062)
# ============================================================================

def load_curation_config(config_path):
    """Load curation configuration with defaults (CUR-001, CUR-031, CUR-062).

    Returns both curation settings and database path for connection.
    """
    config_path = Path(config_path)

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(json.dumps({
            'error': f'Configuration file not found: {config_path}',
            'exit_code': 2
        }))
        sys.exit(2)
    except Exception as e:
        print(json.dumps({
            'error': f'Configuration load error: {str(e)}',
            'exit_code': 2
        }))
        sys.exit(2)

    curation = config.get('curation', {})

    # Resolve database path relative to config file location
    # Config is in .context-engine/config/, database is in .context-engine/data/
    context_engine_home = config_path.parent.parent
    db_path = context_engine_home / config['structure']['database_path']

    return {
        'enabled': curation.get('enabled', True),
        'confidence_threshold': curation.get('confidence_threshold', 0.70),
        'domain_migrations': curation.get('domain_migrations', []),
        'conflict_resolution': curation.get('conflict_resolution', 'flag'),
        'archive_scopes': curation.get('archive_scopes', ['historical']),
        'database_path': db_path
    }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_title(title):
    """Normalize title using EXT-033 algorithm (CUR-011).

    Lowercase, hyphen-separated alphanumeric.
    """
    # Lowercase
    normalized = title.lower()
    # Replace non-alphanumeric with hyphens
    normalized = re.sub(r'[^a-z0-9]+', '-', normalized)
    # Remove leading/trailing hyphens
    normalized = normalized.strip('-')
    # Collapse multiple hyphens
    normalized = re.sub(r'-+', '-', normalized)
    return normalized


def load_rule_metadata(conn, rule_id):
    """Load and parse rule metadata JSON."""
    cursor = conn.execute("SELECT metadata FROM rules WHERE id = ?", (rule_id,))
    row = cursor.fetchone()
    if row and row['metadata']:
        return json.loads(row['metadata'])
    return {}


def save_rule_metadata(conn, rule_id, metadata):
    """Save rule metadata as JSON."""
    conn.execute(
        "UPDATE rules SET metadata = ? WHERE id = ?",
        (json.dumps(metadata), rule_id)
    )


def log_verbose(message, verbose):
    """Log to stderr if verbose mode enabled (CUR-083)."""
    if verbose:
        print(message, file=sys.stderr)


# ============================================================================
# SUPERSESSION ENFORCEMENT (CUR-020 through CUR-024)
# ============================================================================

def enforce_supersession(conn, verbose):
    """Set lifecycle='superseded' for rules with superseded_by relationships."""
    changes = []

    log_verbose("[Supersession] Checking rule_relationships table...", verbose)

    # CUR-021: Check rule_relationships table
    cursor = conn.execute("""
        SELECT DISTINCT r.id
        FROM rules r
        JOIN rule_relationships rr ON r.id = rr.from_rule
        WHERE rr.relationship_type = 'superseded_by'
        AND r.lifecycle = 'active'
    """)
    from_table = [row['id'] for row in cursor.fetchall()]

    log_verbose(f"[Supersession] Found {len(from_table)} from table", verbose)

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

    log_verbose(f"[Supersession] Found {len(from_metadata)} from metadata", verbose)

    # CUR-023: Union and update
    to_supersede = set(from_table) | set(from_metadata)
    for rule_id in to_supersede:
        conn.execute(
            "UPDATE rules SET lifecycle = 'superseded' WHERE id = ?",
            (rule_id,)
        )
        changes.append({'action': 'supersede', 'rule': rule_id})
        log_verbose(f"[Supersession] Superseded rule {rule_id}", verbose)

    return changes


# ============================================================================
# EXACT DUPLICATE DETECTION (CUR-010 through CUR-015)
# ============================================================================

def find_exact_duplicates(conn):
    """Find rules with identical (type, domain, normalized_title) tuples (CUR-010)."""
    # First get all active rules
    cursor = conn.execute("""
        SELECT id, type, domain, title
        FROM rules
        WHERE lifecycle = 'active'
    """)

    rules = []
    for row in cursor.fetchall():
        rules.append({
            'id': row['id'],
            'type': row['type'],
            'domain': row['domain'],
            'title': row['title'],
            'normalized_title': normalize_title(row['title'])
        })

    # Group by (type, domain, normalized_title)
    groups = {}
    for rule in rules:
        key = (rule['type'], rule['domain'], rule['normalized_title'])
        if key not in groups:
            groups[key] = []
        groups[key].append(rule['id'])

    # Return only groups with duplicates
    duplicates = []
    for key, rule_ids in groups.items():
        if len(rule_ids) > 1:
            duplicates.append({
                'type': key[0],
                'domain': key[1],
                'normalized_title': key[2],
                'rule_ids': rule_ids
            })

    return duplicates


def merge_duplicates(conn, duplicate_group, verbose):
    """Merge duplicate rules, keeping lowest ID as canonical (CUR-012 through CUR-015)."""
    rule_ids = sorted(duplicate_group['rule_ids'])
    canonical_id = rule_ids[0]
    duplicates_to_delete = rule_ids[1:]

    log_verbose(f"[Duplicates] Merging {len(rule_ids)} duplicates, keeping {canonical_id}", verbose)

    # Update canonical rule metadata (CUR-014, CUR-015)
    metadata = load_rule_metadata(conn, canonical_id)
    metadata['merged_from'] = duplicates_to_delete
    metadata['merged_at'] = datetime.now(UTC).isoformat().replace('+00:00', 'Z')
    save_rule_metadata(conn, canonical_id, metadata)

    # Delete duplicates (CUR-013)
    for dup_id in duplicates_to_delete:
        conn.execute("DELETE FROM rules WHERE id = ?", (dup_id,))
        log_verbose(f"[Duplicates] Deleted duplicate {dup_id}", verbose)

    return {'action': 'merge', 'kept': canonical_id, 'deleted': duplicates_to_delete}


def process_all_duplicates(conn, verbose):
    """Find and merge all exact duplicate groups."""
    changes = []
    duplicates = find_exact_duplicates(conn)

    log_verbose(f"[Duplicates] Found {len(duplicates)} duplicate groups", verbose)

    for duplicate_group in duplicates:
        change = merge_duplicates(conn, duplicate_group, verbose)
        changes.append(change)

    return changes


# ============================================================================
# CONFIDENCE THRESHOLD ENFORCEMENT (CUR-030 through CUR-035)
# ============================================================================

def archive_low_confidence(conn, threshold, verbose):
    """Archive rules below confidence threshold (CUR-030 through CUR-035)."""
    log_verbose(f"[Confidence] Checking rules below threshold {threshold}", verbose)

    cursor = conn.execute("""
        SELECT id, confidence, metadata FROM rules
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

        changes.append({
            'action': 'archive',
            'rule': row['id'],
            'reason': 'low_confidence',
            'confidence': row['confidence']
        })
        log_verbose(f"[Confidence] Archived rule {row['id']} (confidence={row['confidence']})", verbose)

    return changes


# ============================================================================
# DOMAIN MIGRATION (CUR-040 through CUR-045)
# ============================================================================

def apply_domain_migrations(conn, migrations, verbose):
    """Apply domain renames from configuration (CUR-040 through CUR-045)."""
    changes = []
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    log_verbose(f"[Domains] Applying {len(migrations)} migrations", verbose)

    # CUR-043: Apply in array order
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
            log_verbose(f"[Domains] Migrated rule {row['id']} from {from_domain} to {to_domain}", verbose)

    return changes


# ============================================================================
# SCOPE-BASED ARCHIVAL (CUR-060 through CUR-063)
# ============================================================================

def archive_excluded_scopes(conn, archive_scopes, verbose):
    """Archive rules with reusability_scope in excluded list (CUR-060 through CUR-063)."""
    changes = []
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    log_verbose(f"[Scopes] Archiving scopes: {archive_scopes}", verbose)

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
            log_verbose(f"[Scopes] Archived rule {row['id']} with scope {scope}", verbose)

    return changes


# ============================================================================
# CONFLICT DETECTION (CUR-050 through CUR-057)
# ============================================================================

def detect_conflicts(conn, resolution_strategy, verbose):
    """Detect and optionally resolve rule conflicts (CUR-050 through CUR-057)."""
    changes = []
    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    log_verbose(f"[Conflicts] Using resolution strategy: {resolution_strategy}", verbose)

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

    log_verbose(f"[Conflicts] Found {len(conflicts_from_table)} from table", verbose)

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

    log_verbose(f"[Conflicts] Found {len(conflicts_from_metadata)} from metadata", verbose)

    # Union all conflicts
    all_conflicts = set(conflicts_from_table) | set(conflicts_from_metadata)

    for rule_a, rule_b in all_conflicts:
        # CUR-054: Default strategy is 'flag'
        if resolution_strategy == 'flag':
            changes.append({
                'action': 'conflict_flagged',
                'rules': [rule_a, rule_b],
                'resolution': 'manual_required'
            })
            log_verbose(f"[Conflicts] Flagged conflict between {rule_a} and {rule_b}", verbose)

        elif resolution_strategy == 'keep_newer':
            # CUR-055: Archive rule with earlier created_at
            cursor = conn.execute("""
                SELECT id, created_at, metadata FROM rules
                WHERE id IN (?, ?)
                ORDER BY created_at DESC
            """, (rule_a, rule_b))
            rows = cursor.fetchall()

            if len(rows) < 2:
                continue

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
            log_verbose(f"[Conflicts] Resolved conflict: kept {newer['id']}, archived {older['id']}", verbose)

        elif resolution_strategy == 'keep_higher_confidence':
            # CUR-056: Archive rule with lower confidence
            cursor = conn.execute("""
                SELECT id, confidence, metadata FROM rules
                WHERE id IN (?, ?)
                ORDER BY confidence DESC NULLS LAST
            """, (rule_a, rule_b))
            rows = cursor.fetchall()

            if len(rows) < 2:
                continue

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
            log_verbose(f"[Conflicts] Resolved conflict: kept {higher['id']}, archived {lower['id']}", verbose)

    return changes


# ============================================================================
# OUTPUT FORMATTING (CUR-070 through CUR-074)
# ============================================================================

def build_result(changes, mode, config, run_id):
    """Build JSON output with stats (CUR-071 through CUR-073)."""
    stats = {
        'rules_processed': 0,
        'duplicates_merged': 0,
        'supersessions_enforced': 0,
        'archived_low_confidence': 0,
        'domains_migrated': 0,
        'conflicts_detected': 0,
        'conflicts_resolved': 0,
        'scopes_archived': 0
    }

    for change in changes:
        action = change['action']
        if action == 'merge':
            stats['duplicates_merged'] += len(change['deleted'])
        elif action == 'supersede':
            stats['supersessions_enforced'] += 1
        elif action == 'archive':
            if change.get('reason') == 'low_confidence':
                stats['archived_low_confidence'] += 1
            elif change.get('reason') == 'scope_excluded':
                stats['scopes_archived'] += 1
        elif action == 'domain_migrate':
            stats['domains_migrated'] += 1
        elif action == 'conflict_flagged':
            stats['conflicts_detected'] += 1
        elif action == 'conflict_resolved':
            stats['conflicts_resolved'] += 1

    return {
        'run_id': run_id,
        'mode': mode,
        'config': {
            'confidence_threshold': config['confidence_threshold'],
            'conflict_resolution': config['conflict_resolution'],
            'domain_migrations_count': len(config['domain_migrations']),
            'archive_scopes': config['archive_scopes']
        },
        'stats': stats,
        'changes': changes,
        'warnings': [],
        'errors': [],
        'exit_code': 0
    }


def output_result(result):
    """Output JSON result to stdout (CUR-084)."""
    print(json.dumps(result, indent=2))


# ============================================================================
# CLI INTERFACE (CUR-080 through CUR-084)
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Automated rule curation for CI/CD pipeline'
    )
    parser.add_argument(
        '--mode',
        choices=['apply', 'dry-run'],
        default='apply',
        help='Execution mode: apply changes or dry-run preview (default: apply)'
    )
    parser.add_argument(
        '--config',
        default=None,
        help='Path to deployment.yaml (default: auto-detect from script location)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging to stderr'
    )
    return parser.parse_args()


# ============================================================================
# MAIN EXECUTION (CUR-002, CUR-003, CUR-090, CUR-091)
# ============================================================================

def main():
    """Automated rule curation for CI/CD pipeline execution."""
    args = parse_args()

    # Determine config path
    if args.config:
        config_path = Path(args.config)
    else:
        # Auto-detect from script location
        script_dir = Path(__file__).parent
        config_path = script_dir.parent / "config" / "deployment.yaml"

    log_verbose(f"Loading configuration from {config_path}", args.verbose)

    # Load configuration (CUR-001)
    config = load_curation_config(config_path)

    now = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

    # Check if curation is enabled
    if not config['enabled']:
        output_result({
            'run_id': now,
            'mode': args.mode,
            'stats': {},
            'changes': [],
            'message': 'curation disabled in configuration',
            'exit_code': 0
        })
        return 0

    # Connect to database
    db_path = config['database_path']

    if not db_path.exists():
        output_result({
            'run_id': now,
            'mode': args.mode,
            'error': f'Database not found: {db_path}',
            'exit_code': 2
        })
        return 2

    log_verbose(f"Connecting to database: {db_path}", args.verbose)

    # CUR-002: Single transaction
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        changes = []

        # CUR-091: Fixed execution order
        log_verbose("=== Phase 1: Supersession Enforcement ===", args.verbose)
        changes.extend(enforce_supersession(conn, args.verbose))

        log_verbose("=== Phase 2: Duplicate Detection ===", args.verbose)
        changes.extend(process_all_duplicates(conn, args.verbose))

        log_verbose("=== Phase 3: Confidence Threshold ===", args.verbose)
        changes.extend(archive_low_confidence(conn, config['confidence_threshold'], args.verbose))

        log_verbose("=== Phase 4: Domain Migration ===", args.verbose)
        changes.extend(apply_domain_migrations(conn, config['domain_migrations'], args.verbose))

        log_verbose("=== Phase 5: Scope Archival ===", args.verbose)
        changes.extend(archive_excluded_scopes(conn, config['archive_scopes'], args.verbose))

        log_verbose("=== Phase 6: Conflict Detection ===", args.verbose)
        changes.extend(detect_conflicts(conn, config['conflict_resolution'], args.verbose))

        # CUR-081: Dry-run mode
        if args.mode == 'apply':
            conn.commit()
            log_verbose("=== Changes committed ===", args.verbose)
        else:
            conn.rollback()
            log_verbose("=== Dry-run: no changes committed ===", args.verbose)

        # CUR-070: Output JSON to stdout
        output_result(build_result(changes, args.mode, config, now))
        return 0

    except Exception as e:
        # CUR-003: Rollback on error
        conn.rollback()
        log_verbose(f"Error during curation: {e}", args.verbose)
        output_result({
            'run_id': now,
            'mode': args.mode,
            'error': str(e),
            'exit_code': 1
        })
        return 1

    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
