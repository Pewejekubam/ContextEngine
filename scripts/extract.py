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
    """Extract rules from chatlogs into database (EXT-001)."""

    def __init__(self, config):
        """Initialize extractor with configuration."""
        self.config = config

        # EXT-014: Database path from config['structure']['database_path']
        self.db_path = BASE_DIR / config['structure']['database_path']

        # EXT-015: Chatlogs directory from config['structure']['chatlogs_dir']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']

        # EXT-016: Schema directory from config['structure']['schema_dir']
        self.schema_path = BASE_DIR / config['structure']['schema_dir'] / 'schema.sql'

        # EXT-010: Schema version from config['behavior']['chatlog_schema_version']
        self.expected_schema_version = config['behavior']['chatlog_schema_version']

        # EXT-031: Rule ID format and padding from config
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: In-memory counter per type for transaction-safe ID generation
        self.id_counters = {}

        # Connect to database
        self.conn = self._connect_database()

        # EXT-093: Verify schema version before accessing salience column
        self.salience_defaults = self.load_salience_config()
        self.verify_schema_version()

        # Statistics (EXT-071)
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'skipped_chatlogs': 0
        }

    def load_salience_config(self):
        """Load salience defaults from build-constants.yaml (EXT-091)."""
        # Try to load from build-constants.yaml
        build_constants_path = BASE_DIR.parent / 'build' / 'config' / 'build-constants.yaml'
        if build_constants_path.exists():
            try:
                with open(build_constants_path) as f:
                    build_constants = yaml.safe_load(f)
                    return build_constants.get('salience_defaults', {
                        'INV': 0.8, 'ADR': 0.7, 'CON': 0.6, 'PAT': 0.5
                    })
            except Exception:
                pass

        # Fallback to hardcoded defaults
        return {'INV': 0.8, 'ADR': 0.7, 'CON': 0.6, 'PAT': 0.5}

    def verify_schema_version(self):
        """Verify schema version v1.2.0 (EXT-093)."""
        try:
            cursor = self.conn.execute(
                "SELECT value FROM schema_metadata WHERE key = 'schema_version' LIMIT 1"
            )
            row = cursor.fetchone()
            if not row or row[0] != '1.2.0':
                print(f"ERROR: Schema version mismatch. Expected 1.2.0, found {row[0] if row else 'unknown'}", file=sys.stderr)
                print("Please run schema migration or regenerate database.", file=sys.stderr)
                sys.exit(4)
        except sqlite3.OperationalError:
            print("ERROR: schema_metadata table not found. Database schema incompatible.", file=sys.stderr)
            sys.exit(4)

    def _connect_database(self):
        """Connect to database, creating from schema if needed (EXT-006, EXT-007)."""
        db_exists = self.db_path.exists()

        if not db_exists:
            print(f"Database not found, creating from schema: {self.schema_path}")
            # Ensure parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create database from schema
            if not self.schema_path.exists():
                print(f"ERROR: Schema file not found: {self.schema_path}", file=sys.stderr)
                sys.exit(1)

            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row

            with open(self.schema_path) as f:
                schema_sql = f.read()

            conn.executescript(schema_sql)
            conn.commit()
            print("Database created successfully")
        else:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row

        return conn

    def get_unprocessed_chatlogs(self):
        """Find chatlogs not yet processed (EXT-002, EXT-004, EXT-004a)."""
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
        """Normalize topic to URL-safe title (EXT-033, EXT-033a-f)."""
        # Convert to lowercase
        title = topic.lower()

        # Replace non-alphanumeric with hyphens
        title = re.sub(r'[^a-z0-9]+', '-', title)

        # EXT-033b: Strip leading/trailing hyphens
        title = title.strip('-')

        # EXT-033c: Replace consecutive hyphens with single hyphen
        title = re.sub(r'-+', '-', title)

        # EXT-033e, EXT-033f: Truncate at 100 characters with ellipsis
        if len(title) > 100:
            title = title[:97] + '...'

        return title

    def get_next_rule_id(self, rule_type):
        """Generate next unique rule ID (EXT-030, EXT-030a, EXT-031, EXT-031a, EXT-032)."""
        # Map rule type to prefix
        type_map = {
            'decision': 'ADR',
            'constraint': 'CON',
            'invariant': 'INV'
        }
        prefix = type_map[rule_type]

        # EXT-030a: Initialize counter on first call for this type
        if prefix not in self.id_counters:
            cursor = self.conn.execute(
                "SELECT id FROM rules WHERE type = ? ORDER BY id DESC LIMIT 1",
                (prefix,)
            )
            row = cursor.fetchone()
            if row:
                # Extract sequence number from ID (e.g., "CON-00073" -> 73)
                match = re.search(r'\d+', row['id'])
                seq = int(match.group()) + 1 if match else 1
            else:
                seq = 1
            self.id_counters[prefix] = seq

        # Allocate next ID from counter
        seq = self.id_counters[prefix]
        self.id_counters[prefix] += 1

        # EXT-031, EXT-031a: Format using template with string replacement
        # Template format: '{TYPE}-{NNNNN}' with configurable padding
        padded_seq = str(seq).zfill(self.rule_id_padding)
        rule_id = self.rule_id_format.replace('{TYPE}', prefix).replace('{NNNNN}', padded_seq)

        return rule_id

    def resolve_reusability_scope(self, category_key, rule_index, reusability_scope_map):
        """Resolve rule scope from chatlog references (EXT-065, EXT-066, EXT-067, EXT-068)."""
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
        """Assign default salience if not already set (EXT-091, EXT-092)."""
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
        """Process single chatlog in atomic transaction (EXT-050, EXT-051, EXT-052)."""
        print(f"\nProcessing: {chatlog_path.name}")

        try:
            # Load chatlog
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)

            # EXT-010: Validate schema version
            chatlog_version = chatlog.get('schema_version')
            if chatlog_version != self.expected_schema_version:
                print(f"  ERROR: Schema version mismatch. Expected {self.expected_schema_version}, found {chatlog_version}")
                self.stats['skipped_chatlogs'] += 1
                return False  # EXT-011, EXT-013: Skip without marking processed

            # EXT-012: Validate required fields
            required_fields = ['chatlog_id', 'timestamp', 'rules']
            for field in required_fields:
                if field not in chatlog:
                    print(f"  ERROR: Missing required field: {field}")
                    self.stats['skipped_chatlogs'] += 1
                    return False  # EXT-013: Skip without marking processed

            # EXT-012a: Validate rules field is a dict
            rules_data = chatlog.get('rules', {})
            if not isinstance(rules_data, dict):
                print(f"  ERROR: rules field must be a dict, found {type(rules_data).__name__}")
                self.stats['skipped_chatlogs'] += 1
                return False

            # EXT-065: Load reusability scope map
            session_context = chatlog.get('session_context', {})
            reusability_scope_map = session_context.get('reusability_scope', {})

            # Extract rules from categories
            extracted_rules = []

            # EXT-020: Process decisions, constraints, invariants
            for category_key in ['decisions', 'constraints', 'invariants']:
                category_rules = rules_data.get(category_key, [])

                # EXT-012b: Validate category contains a list
                if not isinstance(category_rules, list):
                    print(f"  WARNING: {category_key} must be a list, found {type(category_rules).__name__}, skipping")
                    continue

                # Use enumerate for reusability scope resolution
                for rule_index, rule in enumerate(category_rules):
                    # EXT-021: Filter by confidence threshold
                    confidence = rule.get('confidence', 0.0)
                    if confidence < 0.5:
                        print(f"  Skipping {category_key}[{rule_index}]: confidence {confidence} < 0.5")
                        continue  # EXT-022

                    # EXT-066: Resolve scope
                    scope = self.resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                    # Build metadata based on category
                    metadata = {'reusability_scope': scope}

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

                    # RREL-008a: Extract implementation references if present
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

                    # Map category to rule type
                    type_map = {
                        'decisions': 'decision',
                        'constraints': 'constraint',
                        'invariants': 'invariant'
                    }
                    rule_type = type_map[category_key]

                    # EXT-030: Generate unique ID
                    rule_id = self.get_next_rule_id(rule_type)

                    # EXT-033: Normalize title
                    title = self.normalize_title(rule.get('topic', 'untitled'))

                    # Map type to prefix for database
                    prefix_map = {
                        'decision': 'ADR',
                        'constraint': 'CON',
                        'invariant': 'INV'
                    }

                    # Build rule record
                    rule_record = {
                        'id': rule_id,
                        'type': prefix_map[rule_type],
                        'title': title,
                        'description': rule.get('rationale', ''),  # EXT-034
                        'domain': rule.get('domain', ''),
                        'confidence': confidence,
                        'tags': '[]',  # EXT-040: Empty tags array
                        'tags_state': 'needs_tags',  # EXT-041
                        'lifecycle': 'active',
                        'chatlog_id': chatlog['chatlog_id'],  # EXT-060
                        'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                        'metadata': json.dumps(metadata),
                        'salience': None  # Will be set by assign_default_salience
                    }

                    # EXT-091, EXT-092: Assign salience
                    self.assign_default_salience(rule_record)

                    extracted_rules.append(rule_record)

                    # Update stats
                    self.stats['total_rules'] += 1
                    self.stats['rules_by_type'][prefix_map[rule_type]] += 1
                    self.stats['total_confidence'] += confidence

            # EXT-023: Even if no qualifying rules, mark as processed
            if not extracted_rules:
                print(f"  No qualifying rules found (confidence >= 0.5)")

            # EXT-050a: Do not use explicit BEGIN - sqlite3 auto-starts transactions
            try:
                # Insert chatlog record (EXT-008, EXT-060)
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
                for rule in extracted_rules:
                    self.conn.execute(
                        """INSERT INTO rules (id, type, title, description, domain, confidence,
                           tags_state, lifecycle, tags, chatlog_id, created_at, metadata, salience)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            rule['id'],
                            rule['type'],
                            rule['title'],
                            rule['description'],
                            rule['domain'],
                            rule['confidence'],
                            rule['tags_state'],
                            rule['lifecycle'],
                            rule['tags'],
                            rule['chatlog_id'],
                            rule['created_at'],
                            rule['metadata'],
                            rule['salience']
                        )
                    )

                # EXT-050: Commit transaction
                self.conn.commit()

                print(f"  ✓ Extracted {len(extracted_rules)} rules")
                self.stats['total_chatlogs'] += 1
                return True

            except Exception as e:
                # EXT-052: Rollback on failure
                self.conn.rollback()
                # EXT-053: Log and continue
                print(f"  ERROR: Transaction failed: {e}", file=sys.stderr)
                self.stats['skipped_chatlogs'] += 1
                return False

        except Exception as e:
            print(f"  ERROR: Failed to process chatlog: {e}", file=sys.stderr)
            self.stats['skipped_chatlogs'] += 1
            return False

    def run(self):
        """Main extraction loop (EXT-001, EXT-002, EXT-003, EXT-070, EXT-071)."""
        print(f"Chatlog directory: {self.chatlogs_dir}")
        print(f"Database: {self.db_path}")
        print(f"Expected schema version: {self.expected_schema_version}")

        # EXT-002, EXT-004: Get unprocessed chatlogs in chronological order
        unprocessed = self.get_unprocessed_chatlogs()

        if not unprocessed:
            print("\n✓ No unprocessed chatlogs found")
            return True  # EXT-005

        print(f"\nFound {len(unprocessed)} unprocessed chatlog(s)")

        # Process each chatlog
        for chatlog_path in unprocessed:
            self.process_chatlog(chatlog_path)  # EXT-070: Per-chatlog progress

        # EXT-071: Summary statistics
        print("\n" + "="*70)
        print("Extraction Summary:")
        print(f"  Total chatlogs processed: {self.stats['total_chatlogs']}")
        print(f"  Total chatlogs skipped: {self.stats['skipped_chatlogs']}")
        print(f"  Total rules extracted: {self.stats['total_rules']}")
        print(f"  Rules by type:")
        print(f"    ADR (Decisions): {self.stats['rules_by_type']['ADR']}")
        print(f"    CON (Constraints): {self.stats['rules_by_type']['CON']}")
        print(f"    INV (Invariants): {self.stats['rules_by_type']['INV']}")

        if self.stats['total_rules'] > 0:
            avg_confidence = self.stats['total_confidence'] / self.stats['total_rules']
            print(f"  Average confidence: {avg_confidence:.2f}")

        return True

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Transform chatlogs into database rules with validation and provenance"""
    print("Context Engine - Extract (Chatlog to Database ETL)")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)  # EXT-072

    # Run extraction
    extractor = ChatlogExtractor(config)
    try:
        success = extractor.run()
        extractor.close()

        # EXT-072: Exit with success (0) or failure (1)
        return 0 if success else 1
    except Exception as e:
        print(f"\nFATAL ERROR: {e}", file=sys.stderr)
        extractor.close()
        return 1


if __name__ == '__main__':
    sys.exit(main())
