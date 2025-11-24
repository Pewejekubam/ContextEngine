#!/usr/bin/env python3
"""
Chatlog to database ETL script with pure extraction (no tag normalization)

Implements constraints: EXT-001 through EXT-093
Generated from: specs/modules/runtime-script-etl-extract-v1.4.0.yaml
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


class ChatlogExtractor:
    """ETL processor for transforming chatlogs into database rules"""

    def __init__(self, config):
        """Initialize extractor with configuration"""
        self.config = config

        # EXT-014, EXT-015, EXT-016: Read paths from config['structure']
        self.db_path = BASE_DIR / config['structure']['database_path']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']
        self.schema_path = BASE_DIR / config['structure']['schema_dir'] / 'schema.sql'

        # EXT-010: Get expected schema version from config
        self.expected_schema_version = config['behavior']['chatlog_schema_version']

        # EXT-031: Get rule ID formatting settings
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: Initialize ID counters for in-transaction tracking
        self.id_counters = {}

        # Database connection (initialized later)
        self.conn = None

        # Statistics tracking
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'skipped_chatlogs': 0,
            'filtered_rules': 0
        }

    def load_salience_config(self):
        """Load salience defaults from config (EXT-091, SAL-009)"""
        # Try to load from config first
        salience_defaults = self.config.get('salience_defaults', {})

        # Fallback to hardcoded defaults if not in config
        if not salience_defaults:
            salience_defaults = {
                'INV': 0.8,
                'ADR': 0.7,
                'CON': 0.6,
                'PAT': 0.5
            }

        return salience_defaults

    def verify_schema_version(self):
        """Verify database schema version v1.2.0 (EXT-093, SAL-012)"""
        try:
            cursor = self.conn.execute(
                "SELECT value FROM schema_metadata WHERE key = 'schema_version' LIMIT 1"
            )
            row = cursor.fetchone()

            if not row or row[0] != '1.2.0':
                print(
                    f"ERROR: Schema version mismatch. Expected 1.2.0, found {row[0] if row else 'unknown'}",
                    file=sys.stderr
                )
                print("Please run schema migration or regenerate database.", file=sys.stderr)
                sys.exit(4)
        except sqlite3.OperationalError:
            print("ERROR: schema_metadata table not found. Database schema incompatible.", file=sys.stderr)
            sys.exit(4)

    def initialize_database(self):
        """Initialize database connection and create schema if needed (EXT-006, EXT-007)"""
        # EXT-006: Create database from schema if it doesn't exist
        if not self.db_path.exists():
            print(f"Creating new database: {self.db_path}")
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Read schema file
            if not self.schema_path.exists():
                print(f"ERROR: Schema file not found: {self.schema_path}", file=sys.stderr)
                sys.exit(1)

            with open(self.schema_path) as f:
                schema_sql = f.read()

            # Create database
            conn = sqlite3.connect(str(self.db_path))
            conn.executescript(schema_sql)
            conn.commit()
            conn.close()
            print("Database created successfully")

        # Open connection with row factory
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

        # EXT-093: Verify schema version before proceeding
        self.verify_schema_version()

        # Load salience defaults
        self.salience_defaults = self.load_salience_config()

    def get_unprocessed_chatlogs(self):
        """Find chatlogs not yet processed (EXT-002, EXT-004, EXT-004a)"""
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

    def normalize_title(self, topic):
        """Create URL-safe title from topic (EXT-033, EXT-033a-f)"""
        # Convert to lowercase and replace non-alphanumeric with hyphens
        title = topic.lower()
        title = re.sub(r'[^a-z0-9]+', '-', title)

        # EXT-033b: Remove leading/trailing hyphens
        title = title.strip('-')

        # EXT-033c: Replace consecutive hyphens with single hyphen
        title = re.sub(r'-+', '-', title)

        # EXT-033e, EXT-033f: Truncate at 100 characters with ellipsis
        if len(title) > 100:
            title = title[:97] + '...'

        return title

    def format_rule_id(self, rule_type, sequence):
        """Format rule ID using template (EXT-031, EXT-031a)"""
        # EXT-031a: Use string replacement to avoid Python keyword conflicts
        padded_seq = str(sequence).zfill(self.rule_id_padding)
        rule_id = self.rule_id_format.replace('{TYPE}', rule_type)
        rule_id = rule_id.replace('{NNNNN}', padded_seq)
        return rule_id

    def get_next_rule_id(self, rule_type):
        """Generate next sequential rule ID (EXT-030, EXT-030a, EXT-032)"""
        # Map category names to rule type prefixes
        type_map = {
            'decisions': 'ADR',
            'constraints': 'CON',
            'invariants': 'INV'
        }
        prefix = type_map.get(rule_type, rule_type)

        # EXT-030a: Initialize counter on first call for this type
        if prefix not in self.id_counters:
            # Query database for current max
            cursor = self.conn.execute(
                "SELECT id FROM rules WHERE type = ? ORDER BY id DESC LIMIT 1",
                (prefix,)
            )
            row = cursor.fetchone()

            if row:
                # Extract number from ID (e.g., "CON-00073" -> 73)
                match = re.search(r'\d+', row['id'])
                seq = int(match.group()) + 1 if match else 1
            else:
                seq = 1

            self.id_counters[prefix] = seq

        # Allocate next ID from counter
        seq = self.id_counters[prefix]
        self.id_counters[prefix] += 1

        return self.format_rule_id(prefix, seq)

    def resolve_reusability_scope(self, category_key, rule_index, reusability_scope_map):
        """Resolve rule scope from chatlog references (EXT-065, EXT-066, EXT-067, EXT-068)"""
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

    def assign_default_salience(self, rule):
        """Assign default salience if not already set (EXT-091, EXT-092)"""
        metadata = rule.get('metadata') or {}

        # EXT-092: Skip if already assigned
        if 'salience_method' in metadata:
            return

        # EXT-091: Assign default
        if rule.get('salience') is None:
            rule_type = rule['type']
            rule['salience'] = self.salience_defaults.get(rule_type, 0.5)
            metadata['salience_method'] = 'default'
            rule['metadata'] = metadata

    def process_chatlog(self, chatlog_path):
        """Process single chatlog file (EXT-001, EXT-050-053, EXT-070)"""
        print(f"\nProcessing: {chatlog_path.name}")

        try:
            # Load chatlog YAML
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)

            # EXT-010: Validate schema version
            chatlog_version = chatlog.get('schema_version', '')
            if chatlog_version != self.expected_schema_version:
                # EXT-011: Skip with error logged
                print(f"  ERROR: Schema version mismatch. Expected {self.expected_schema_version}, got {chatlog_version}")
                self.stats['skipped_chatlogs'] += 1
                return False  # EXT-013: Not marked as processed

            # EXT-012: Validate required fields
            required_fields = ['chatlog_id', 'timestamp', 'rules']
            for field in required_fields:
                if field not in chatlog:
                    print(f"  ERROR: Missing required field: {field}")
                    self.stats['skipped_chatlogs'] += 1
                    return False

            # EXT-012a: Validate rules field is a dict
            rules = chatlog.get('rules', {})
            if not isinstance(rules, dict):
                print(f"  ERROR: rules field must be dict, got {type(rules).__name__}")
                self.stats['skipped_chatlogs'] += 1
                return False

            # EXT-065: Load reusability scope map
            session_context = chatlog.get('session_context', {})
            reusability_scope_map = session_context.get('reusability_scope', {})

            # EXT-050, EXT-050a: Process in atomic transaction (no explicit BEGIN)
            # sqlite3 module automatically starts transaction

            # Collect all rules to insert
            rules_to_insert = []

            # EXT-020: Extract from all categories
            for category_key in ['decisions', 'constraints', 'invariants']:
                category_rules = rules.get(category_key, [])

                # EXT-012b: Validate category is a list
                if not isinstance(category_rules, list):
                    print(f"  WARNING: {category_key} must be list, got {type(category_rules).__name__}, skipping")
                    continue

                # Process each rule with index for scope resolution
                for rule_index, rule in enumerate(category_rules):
                    # EXT-021: Filter by confidence threshold
                    confidence = rule.get('confidence', 0.0)
                    if confidence < 0.5:
                        # EXT-022: Log filtered rules
                        print(f"  Filtered: {rule.get('topic', 'unknown')} (confidence={confidence})")
                        self.stats['filtered_rules'] += 1
                        continue

                    # Generate unique rule ID
                    rule_id = self.get_next_rule_id(category_key)

                    # Get rule type prefix
                    type_map = {'decisions': 'ADR', 'constraints': 'CON', 'invariants': 'INV'}
                    rule_type = type_map[category_key]

                    # EXT-033, EXT-034: Transform fields
                    title = self.normalize_title(rule.get('topic', 'untitled'))
                    description = rule.get('rationale', '')
                    domain = rule.get('domain', '')

                    # EXT-066: Resolve reusability scope
                    scope = self.resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                    # Build metadata JSON
                    metadata = {'reusability_scope': scope}

                    # Add category-specific metadata fields
                    if category_key == 'decisions':
                        metadata['alternatives_rejected'] = rule.get('alternatives_rejected', [])
                    elif category_key == 'constraints':
                        metadata['validation_method'] = rule.get('validation_method', '')

                    # RREL-004: Extract relationships if present
                    if 'relationships' in rule:
                        metadata['relationships'] = [
                            {
                                'type': rel['type'],
                                'target': rel['target'],
                                'rationale': rel['rationale'],
                                'created_at': chatlog.get('metadata', {}).get('captured_at', chatlog['timestamp'])
                            }
                            for rel in rule['relationships']
                        ]

                    # RREL-008a: Extract implementation_refs if present
                    if 'implementation_refs' in rule:
                        metadata['implementation_refs'] = [
                            {
                                'type': ref['type'],
                                'file': ref['file'],
                                'lines': ref.get('lines'),
                                'role_description': ref['role_description'],
                                'created_at': chatlog.get('metadata', {}).get('captured_at', chatlog['timestamp'])
                            }
                            for ref in rule['implementation_refs']
                        ]

                    # Create rule record
                    rule_record = {
                        'id': rule_id,
                        'type': rule_type,
                        'title': title,
                        'description': description,
                        'domain': domain,
                        'confidence': confidence,
                        'salience': None,  # Will be set by assign_default_salience
                        'tags_state': 'needs_tags',  # EXT-041
                        'lifecycle': 'active',
                        'tags': '[]',  # EXT-040: Empty tags array
                        'chatlog_id': chatlog['chatlog_id'],
                        'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                        'metadata': metadata
                    }

                    # EXT-091, EXT-092: Assign salience
                    self.assign_default_salience(rule_record)

                    rules_to_insert.append(rule_record)

                    # Update statistics
                    self.stats['total_rules'] += 1
                    self.stats['rules_by_type'][rule_type] += 1
                    self.stats['total_confidence'] += confidence

            # EXT-023: Mark as processed even if no qualifying rules
            if len(rules_to_insert) == 0:
                print(f"  No qualifying rules (all filtered or no rules)")
            else:
                print(f"  Extracted {len(rules_to_insert)} rules")

            # Insert chatlog record (EXT-060)
            self.conn.execute(
                """INSERT INTO chatlogs (chatlog_id, filename, timestamp, schema_version, agent, processed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    chatlog['chatlog_id'],
                    chatlog_path.name,
                    chatlog['timestamp'],
                    chatlog['schema_version'],
                    chatlog.get('agent', 'unknown'),
                    datetime.now(UTC).isoformat().replace('+00:00', 'Z')
                )
            )

            # Insert rule records
            for rule_record in rules_to_insert:
                metadata_json = json.dumps(rule_record['metadata']) if rule_record['metadata'] else None

                self.conn.execute(
                    """INSERT INTO rules
                       (id, type, title, description, domain, confidence, salience, tags_state,
                        lifecycle, tags, chatlog_id, created_at, metadata)
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
                        metadata_json
                    )
                )

            # EXT-050, EXT-051: Commit transaction
            self.conn.commit()

            self.stats['total_chatlogs'] += 1
            return True

        except Exception as e:
            # EXT-052, EXT-053: Rollback on failure, log and continue
            print(f"  ERROR: Failed to process chatlog: {e}", file=sys.stderr)
            self.conn.rollback()
            self.stats['skipped_chatlogs'] += 1
            return False

    def run(self):
        """Main extraction workflow (EXT-001-005, EXT-070-072)"""
        print("\nInitializing database...")
        self.initialize_database()

        # EXT-004: Get unprocessed chatlogs
        print("\nScanning for unprocessed chatlogs...")
        unprocessed = self.get_unprocessed_chatlogs()

        if not unprocessed:
            # EXT-005: Success when no unprocessed chatlogs remain
            print("No unprocessed chatlogs found.")
            self.print_summary()
            return 0

        print(f"Found {len(unprocessed)} unprocessed chatlog(s)")

        # EXT-002: Process in chronological order (already sorted)
        for chatlog_path in unprocessed:
            self.process_chatlog(chatlog_path)

        # EXT-071: Print summary
        self.print_summary()

        # EXT-072: Exit with success (0) or failure (1)
        if self.stats['skipped_chatlogs'] > 0:
            print(f"\nWARNING: {self.stats['skipped_chatlogs']} chatlog(s) skipped due to errors")
            return 1

        return 0

    def print_summary(self):
        """Print extraction summary (EXT-071)"""
        print("\n" + "="*70)
        print("EXTRACTION SUMMARY")
        print("="*70)
        print(f"Total chatlogs processed: {self.stats['total_chatlogs']}")
        print(f"Total rules extracted:    {self.stats['total_rules']}")
        print(f"  Decisions (ADR):        {self.stats['rules_by_type']['ADR']}")
        print(f"  Constraints (CON):      {self.stats['rules_by_type']['CON']}")
        print(f"  Invariants (INV):       {self.stats['rules_by_type']['INV']}")

        if self.stats['total_rules'] > 0:
            avg_confidence = self.stats['total_confidence'] / self.stats['total_rules']
            print(f"Average confidence:       {avg_confidence:.2f}")

        if self.stats['filtered_rules'] > 0:
            print(f"\nFiltered rules (low confidence): {self.stats['filtered_rules']}")

        if self.stats['skipped_chatlogs'] > 0:
            print(f"Skipped chatlogs (errors): {self.stats['skipped_chatlogs']}")


def main():
    """Transform chatlogs into database rules with validation and provenance"""
    print("Context Engine - ETL Extract")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Run extraction
    extractor = ChatlogExtractor(config)
    return extractor.run()


if __name__ == '__main__':
    sys.exit(main())
