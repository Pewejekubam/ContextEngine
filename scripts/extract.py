#!/usr/bin/env python3
"""
Chatlog to database ETL script with pure extraction (no tag normalization)

Implements constraints: EXT-001 through EXT-093
Generated from: specs/modules/runtime-script-etl-extract-v1.4.1.yaml
"""

import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, UTC
import re

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
# RUNTIME-SCRIPT-ETL-EXTRACT MODULE IMPLEMENTATION
# ============================================================================


def normalize_title(topic):
    """Normalize rule topic to URL-safe title (EXT-033).

    Args:
        topic: Original topic string

    Returns:
        URL-safe normalized title (EXT-033a-f)
    """
    # Convert to lowercase
    title = topic.lower()

    # Replace non-alphanumeric with hyphens (EXT-033a)
    title = re.sub(r'[^a-z0-9]+', '-', title)

    # Remove leading/trailing hyphens (EXT-033b)
    title = title.strip('-')

    # Collapse consecutive hyphens (EXT-033c)
    title = re.sub(r'-+', '-', title)

    # Truncate at 100 characters (EXT-033e)
    if len(title) > 100:
        # Find last hyphen before character 97 to avoid cutting words
        truncate_pos = title.rfind('-', 0, 97)
        if truncate_pos == -1:
            truncate_pos = 97
        title = title[:truncate_pos] + '...'  # EXT-033f

    return title


def resolve_reusability_scope(category_key, rule_index, reusability_scope_map):
    """Resolve rule scope from chatlog references (EXT-065, EXT-066, EXT-067).

    Args:
        category_key: 'decisions', 'constraints', or 'invariants'
        rule_index: 0-based index within category
        reusability_scope_map: dict from session_context.reusability_scope

    Returns:
        'project_wide', 'module_scoped', or 'historical'
    """
    rule_ref = f"{category_key}[{rule_index}]"

    # Check explicit mappings
    if rule_ref in reusability_scope_map.get('project_wide', []):
        return 'project_wide'

    if rule_ref in reusability_scope_map.get('historical', []):
        return 'historical'

    # Check module_scoped dict
    for module, refs in reusability_scope_map.get('module_scoped', {}).items():
        if rule_ref in refs:
            return 'module_scoped'

    # EXT-068: Default to project_wide
    return 'project_wide'


class ChatlogExtractor:
    """Transform chatlogs into database rules with validation and provenance."""

    def __init__(self, config):
        """Initialize extractor with configuration.

        Args:
            config: Deployment configuration dict
        """
        self.config = config

        # EXT-014: Database path from config['structure']['database_path']
        self.db_path = BASE_DIR / config['structure']['database_path']

        # EXT-015: Chatlogs directory from config['structure']['chatlogs_dir']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']

        # EXT-016: Schema directory from config['structure']['schema_dir']
        self.schema_path = BASE_DIR / config['structure']['schema_dir'] / 'schema.sql'

        # EXT-010: Chatlog schema version from config['behavior']['chatlog_schema_version']
        self.expected_schema_version = config['behavior']['chatlog_schema_version']

        # EXT-031: Rule ID format and padding from config
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: In-memory ID counters per type
        self.id_counters = {}

        # Database connection
        self.conn = None

        # Statistics tracking (EXT-071)
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'filtered_rules': 0
        }

        # EXT-093: Load salience configuration and verify schema
        self.salience_defaults = self.load_salience_config()

    def load_salience_config(self):
        """Load salience defaults from build-constants.yaml (EXT-091).

        Returns:
            dict: Salience defaults by rule type
        """
        # Try to load from build-constants.yaml
        build_constants_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'

        if build_constants_path.exists():
            try:
                with open(build_constants_path) as f:
                    build_constants = yaml.safe_load(f)
                    return build_constants.get('salience_defaults', {
                        'INV': 0.8,
                        'ADR': 0.7,
                        'CON': 0.6,
                        'PAT': 0.5
                    })
            except Exception:
                pass

        # Fallback to hardcoded defaults (Spec 27 SAL-001)
        return {
            'INV': 0.8,
            'ADR': 0.7,
            'CON': 0.6,
            'PAT': 0.5
        }

    def verify_schema_version(self):
        """Verify schema version v1.2.0 (EXT-093).

        Exits with code 4 if schema version mismatch.
        """
        try:
            cursor = self.conn.execute(
                "SELECT value FROM schema_metadata WHERE key = 'schema_version' LIMIT 1"
            )
            row = cursor.fetchone()
            if not row or row[0] != '1.2.0':
                print(f"ERROR: Schema version mismatch. Expected 1.2.0, found {row[0] if row else 'unknown'}",
                      file=sys.stderr)
                print("Please run schema migration or regenerate database.", file=sys.stderr)
                sys.exit(4)
        except sqlite3.OperationalError:
            print("ERROR: schema_metadata table not found. Database schema incompatible.",
                  file=sys.stderr)
            sys.exit(4)

    def init_database(self):
        """Initialize database connection and create schema if needed (EXT-006, EXT-007)."""
        # Create parent directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if database needs initialization
        db_exists = self.db_path.exists()

        # Connect to database
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

        if not db_exists:
            # EXT-006, EXT-007: Create schema from schema.sql
            print(f"Creating database from schema: {self.schema_path}")
            with open(self.schema_path) as f:
                schema_sql = f.read()
            self.conn.executescript(schema_sql)
            self.conn.commit()

        # EXT-093: Verify schema version before processing
        self.verify_schema_version()

    def get_unprocessed_chatlogs(self):
        """Find chatlogs not yet processed (EXT-002, EXT-004, EXT-004a).

        Returns:
            list: Paths to unprocessed chatlog files
        """
        if not self.chatlogs_dir.exists():
            return []

        chatlog_files = sorted(self.chatlogs_dir.glob('*.yaml'))
        unprocessed = []

        for chatlog_file in chatlog_files:
            # EXT-004a: Load YAML to get chatlog_id (not filename stem)
            try:
                with open(chatlog_file) as f:
                    chatlog = yaml.safe_load(f)
                chatlog_id = chatlog.get('chatlog_id')
                if not chatlog_id:
                    print(f"  WARNING: {chatlog_file.name} missing chatlog_id field, skipping")
                    continue
            except Exception as e:
                print(f"  WARNING: Failed to load {chatlog_file.name}: {e}")
                continue

            # Query database using chatlog_id from YAML
            cursor = self.conn.execute(
                "SELECT processed_at FROM chatlogs WHERE chatlog_id = ?",
                (chatlog_id,)
            )
            row = cursor.fetchone()

            # Unprocessed if: not in database OR processed_at is NULL
            if not row or row['processed_at'] is None:
                unprocessed.append(chatlog_file)

        return unprocessed

    def get_next_rule_id(self, rule_type):
        """Generate next sequential rule ID (EXT-030, EXT-030a, EXT-031, EXT-031a, EXT-032).

        Args:
            rule_type: 'decision', 'constraint', or 'invariant'

        Returns:
            str: Formatted rule ID (e.g., 'ADR-00042')
        """
        # Map chatlog rule types to database types
        type_map = {
            'decision': 'ADR',
            'constraint': 'CON',
            'invariant': 'INV'
        }
        prefix = type_map[rule_type]

        # EXT-030a: Initialize counter on first call for this type
        if prefix not in self.id_counters:
            # Query database for current max
            cursor = self.conn.execute(
                "SELECT id FROM rules WHERE type = ? ORDER BY id DESC LIMIT 1",
                (prefix,)
            )
            row = cursor.fetchone()
            if row:
                # Extract number from ID (e.g., 'ADR-00042' -> 42)
                match = re.search(r'\d+', row['id'])
                if match:
                    seq = int(match.group()) + 1
                else:
                    seq = 1
            else:
                seq = 1
            self.id_counters[prefix] = seq

        # Allocate next ID from counter
        seq = self.id_counters[prefix]
        self.id_counters[prefix] += 1

        # EXT-031, EXT-031a: Format using template
        padded_seq = str(seq).zfill(self.rule_id_padding)
        rule_id = self.rule_id_format.replace('{TYPE}', prefix).replace('{NNNNN}', padded_seq)

        return rule_id

    def assign_default_salience(self, rule_type, metadata):
        """Assign default salience, returning (salience, updated_metadata) tuple (EXT-091, EXT-092).

        Args:
            rule_type: 'ADR', 'CON', or 'INV'
            metadata: dict (NOT JSON string) containing rule metadata

        Returns:
            tuple: (salience_value, metadata_dict_with_salience_method)
        """
        # EXT-092: Skip if already assigned
        if 'salience_method' in metadata:
            return (None, metadata)  # Preserve existing, let caller handle

        # EXT-091: Assign default
        salience = self.salience_defaults.get(rule_type, 0.5)
        metadata['salience_method'] = 'default'
        return (salience, metadata)

    def process_chatlog(self, chatlog_path):
        """Process a single chatlog file (EXT-050, EXT-051, EXT-052, EXT-053).

        Args:
            chatlog_path: Path to chatlog YAML file

        Returns:
            tuple: (success: bool, rules_extracted: int)
        """
        print(f"\nProcessing: {chatlog_path.name}")

        try:
            # Load chatlog
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)

            # EXT-010: Validate schema version
            chatlog_version = chatlog.get('schema_version', 'unknown')
            if chatlog_version != self.expected_schema_version:
                # EXT-011: Skip with error
                print(f"  ERROR: Schema version mismatch. Expected {self.expected_schema_version}, "
                      f"found {chatlog_version}")
                return (False, 0)

            # EXT-012: Validate required fields
            required_fields = ['chatlog_id', 'timestamp', 'schema_version', 'rules']
            for field in required_fields:
                if field not in chatlog:
                    print(f"  ERROR: Missing required field: {field}")
                    return (False, 0)

            # EXT-012a: Validate rules is a dict
            rules_data = chatlog['rules']
            if not isinstance(rules_data, dict):
                print(f"  ERROR: 'rules' field must be a dict, found {type(rules_data).__name__}")
                return (False, 0)

            # Extract metadata
            chatlog_id = chatlog['chatlog_id']

            # EXT-065: Load reusability scope map
            session_context = chatlog.get('session_context', {})
            reusability_scope_map = session_context.get('reusability_scope', {})

            # Metadata for relationships (RREL-004)
            chatlog_captured_at = chatlog.get('metadata', {}).get('captured_at',
                                                                  datetime.now(UTC).isoformat().replace('+00:00', 'Z'))

            # EXT-050: Process in atomic transaction (sqlite3 auto-starts transaction)
            # EXT-050a: Do NOT use explicit BEGIN

            rules_to_insert = []

            # EXT-020: Extract from categories
            categories = {
                'decisions': 'decision',
                'constraints': 'constraint',
                'invariants': 'invariant'
            }

            for category_key, rule_type in categories.items():
                category_rules = rules_data.get(category_key, [])

                # EXT-012b: Validate category is a list
                if not isinstance(category_rules, list):
                    print(f"  WARNING: Category '{category_key}' must be a list, "
                          f"found {type(category_rules).__name__}, skipping")
                    continue

                for rule_index, rule in enumerate(category_rules):
                    # Validate rule structure
                    if not isinstance(rule, dict):
                        print(f"  WARNING: Rule at {category_key}[{rule_index}] is not a dict, skipping")
                        continue

                    # EXT-021: Filter by confidence threshold
                    confidence = rule.get('confidence', 0.0)
                    if confidence < 0.5:
                        # EXT-022: Log filtered rules
                        print(f"  FILTERED: {category_key}[{rule_index}] - confidence {confidence} below threshold 0.5")
                        self.stats['filtered_rules'] += 1
                        continue

                    # Extract rule fields
                    topic = rule.get('topic', 'untitled')
                    rationale = rule.get('rationale', '')
                    domain = rule.get('domain', '')

                    # Generate rule ID
                    rule_id = self.get_next_rule_id(rule_type)

                    # EXT-033: Normalize title
                    title = normalize_title(topic)

                    # EXT-034: Description from rationale
                    description = rationale

                    # Map rule type to database type
                    type_map = {'decision': 'ADR', 'constraint': 'CON', 'invariant': 'INV'}
                    db_type = type_map[rule_type]

                    # EXT-066: Resolve reusability scope
                    scope = resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                    # Build metadata dict
                    metadata = {
                        'reusability_scope': scope
                    }

                    # Add type-specific metadata
                    if rule_type == 'decision':
                        metadata['alternatives_rejected'] = rule.get('alternatives_rejected', [])
                    elif rule_type == 'constraint':
                        metadata['validation_method'] = rule.get('validation_method', '')

                    # RREL-004: Extract relationships
                    if 'relationships' in rule:
                        metadata['relationships'] = [
                            {
                                'type': rel['type'],
                                'target': rel['target'],
                                'rationale': rel['rationale'],
                                'created_at': chatlog_captured_at
                            }
                            for rel in rule['relationships']
                        ]

                    # RREL-008a: Extract implementation references
                    if 'implementation_refs' in rule:
                        metadata['implementation_refs'] = [
                            {
                                'type': ref['type'],
                                'file': ref['file'],
                                'lines': ref.get('lines'),
                                'role_description': ref['role_description'],
                                'created_at': chatlog_captured_at
                            }
                            for ref in rule['implementation_refs']
                        ]

                    # EXT-091, EXT-092: Assign salience BEFORE json.dumps
                    salience, metadata = self.assign_default_salience(db_type, metadata)

                    # Build rule record
                    rule_record = {
                        'id': rule_id,
                        'type': db_type,
                        'title': title,
                        'description': description,
                        'domain': domain,
                        'confidence': confidence,
                        'salience': salience,
                        'tags_state': 'needs_tags',  # EXT-041
                        'lifecycle': 'active',
                        'tags': '[]',  # EXT-040: Empty tags array
                        'chatlog_id': chatlog_id,
                        'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                        'metadata': json.dumps(metadata)
                    }

                    rules_to_insert.append(rule_record)

                    # Update statistics
                    self.stats['rules_by_type'][db_type] += 1
                    self.stats['total_confidence'] += confidence

            # EXT-023: Mark processed even if no qualifying rules
            if len(rules_to_insert) == 0:
                print(f"  No qualifying rules found (all filtered or missing)")
            else:
                print(f"  Extracted {len(rules_to_insert)} rules")

            # Insert chatlog record (EXT-060)
            self.conn.execute(
                """INSERT INTO chatlogs (chatlog_id, filename, timestamp, schema_version, agent, processed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    chatlog_id,
                    chatlog_path.name,
                    chatlog['timestamp'],
                    chatlog['schema_version'],
                    chatlog.get('agent', 'unknown'),
                    datetime.now(UTC).isoformat().replace('+00:00', 'Z')
                )
            )

            # Insert rule records
            for rule_record in rules_to_insert:
                self.conn.execute(
                    """INSERT INTO rules (id, type, title, description, domain, confidence, salience,
                                         tags_state, lifecycle, tags, chatlog_id, created_at, metadata)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        rule_record['id'],
                        rule_record['type'],
                        rule_record['title'],
                        rule_record['description'],
                        rule_record['domain'],
                        rule_record['confidence'],
                        rule_record['salience'],
                        rule_record['tags_state'],
                        rule_record['lifecycle'],
                        rule_record['tags'],
                        rule_record['chatlog_id'],
                        rule_record['created_at'],
                        rule_record['metadata']
                    )
                )

            # EXT-051: Commit transaction
            self.conn.commit()

            # Update statistics
            self.stats['total_chatlogs'] += 1
            self.stats['total_rules'] += len(rules_to_insert)

            return (True, len(rules_to_insert))

        except Exception as e:
            # EXT-052: Rollback on failure
            self.conn.rollback()
            # EXT-053: Log error and continue
            print(f"  ERROR: Failed to process chatlog: {e}")
            return (False, 0)

    def run(self):
        """Main extraction process (EXT-001, EXT-002, EXT-005, EXT-070, EXT-071, EXT-072).

        Returns:
            int: Exit code (0 for success, 1 for failure)
        """
        try:
            # Initialize database
            self.init_database()

            # EXT-004: Get unprocessed chatlogs
            unprocessed = self.get_unprocessed_chatlogs()

            if not unprocessed:
                # EXT-005: Success when no unprocessed remain
                print("\nNo unprocessed chatlogs found.")
                return 0

            print(f"\nFound {len(unprocessed)} unprocessed chatlog(s)")

            # EXT-002: Process in chronological order (sorted by glob)
            for chatlog_path in unprocessed:
                # EXT-070: Report per-chatlog progress
                success, rules_count = self.process_chatlog(chatlog_path)

            # EXT-071: Produce summary
            print("\n" + "="*70)
            print("EXTRACTION SUMMARY")
            print("="*70)
            print(f"Total chatlogs processed: {self.stats['total_chatlogs']}")
            print(f"Total rules extracted: {self.stats['total_rules']}")
            print(f"Rules by type:")
            print(f"  - Decisions (ADR): {self.stats['rules_by_type']['ADR']}")
            print(f"  - Constraints (CON): {self.stats['rules_by_type']['CON']}")
            print(f"  - Invariants (INV): {self.stats['rules_by_type']['INV']}")

            if self.stats['total_rules'] > 0:
                avg_confidence = self.stats['total_confidence'] / self.stats['total_rules']
                print(f"Average confidence: {avg_confidence:.2f}")

            if self.stats['filtered_rules'] > 0:
                print(f"Rules filtered (confidence < 0.5): {self.stats['filtered_rules']}")

            # EXT-072: Exit with success
            return 0

        except Exception as e:
            print(f"\nFATAL ERROR: {e}", file=sys.stderr)
            return 1
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Transform chatlogs into database rules with validation and provenance"""
    print("Context Engine - Chatlog Extraction")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Run extraction
    extractor = ChatlogExtractor(config)
    exit_code = extractor.run()

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
