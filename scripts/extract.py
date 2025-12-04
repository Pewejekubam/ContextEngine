#!/usr/bin/env python3
"""
Chatlog to database ETL script with pure extraction (no tag normalization)

Implements constraints: EXT-001 through EXT-093
Generated from: specs/modules/runtime-script-etl-extract-v1.4.1.yaml
"""

import sys
import json
import sqlite3
import re
import argparse
from pathlib import Path
from datetime import datetime, UTC

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
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


# ============================================================================
# RUNTIME-SCRIPT-ETL-EXTRACT MODULE IMPLEMENTATION
# ============================================================================


class ChatlogExtractor:
    """Chatlog to database ETL processor (EXT-001)."""

    def __init__(self, config, dry_run=False, verbose=False):
        """Initialize extractor with configuration (EXT-014, EXT-015, EXT-016, EXT-093)."""
        self.config = config
        self.dry_run = dry_run
        self.verbose = verbose

        # EXT-014: Database path from config['structure']['database_path']
        self.db_path = BASE_DIR / config['structure']['database_path']

        # EXT-015: Chatlogs directory from config['structure']['chatlogs_dir']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']

        # EXT-016: Schema directory from config['structure']['schema_dir']
        schema_dir = BASE_DIR / config['structure']['schema_dir']
        self.schema_path = schema_dir / 'schema.sql'

        # EXT-010: Expected chatlog schema version
        self.expected_schema_version = config['behavior']['chatlog_schema_version']

        # EXT-031, EXT-031a: Rule ID format configuration
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: ID counters for transaction-safe ID generation
        self.id_counters = {}

        # Connect to database (EXT-006, EXT-007)
        self.conn = self._init_database()

        # EXT-093: Load salience config and verify schema
        self.salience_defaults = self.load_salience_config()
        self.verify_schema_version()

        # Statistics tracking (EXT-071)
        self.stats = {
            'chatlogs_processed': 0,
            'rules_extracted': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'rules_filtered': 0
        }

    def load_salience_config(self):
        """Load salience defaults from build-constants.yaml (EXT-091)."""
        # Try to load from build-constants.yaml
        build_constants_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'

        # Default fallback values
        defaults = {
            'INV': 0.8,
            'ADR': 0.7,
            'CON': 0.6,
            'PAT': 0.5
        }

        try:
            if build_constants_path.exists():
                with open(build_constants_path) as f:
                    build_config = yaml.safe_load(f)
                    if 'salience_defaults' in build_config:
                        defaults.update(build_config['salience_defaults'])
        except Exception as e:
            if self.verbose:
                print(f"  WARNING: Could not load build-constants.yaml, using defaults: {e}")

        return defaults

    def verify_schema_version(self):
        """Verify schema version v1.2.0 (EXT-093)."""
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
            print("ERROR: schema_metadata table not found. Database schema incompatible.", file=sys.stderr)
            sys.exit(4)

    def _init_database(self):
        """Initialize database connection, create schema if needed (EXT-006, EXT-007)."""
        db_exists = self.db_path.exists()

        if not db_exists:
            # Create parent directory if needed
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create database from schema
            if not self.schema_path.exists():
                print(f"ERROR: Schema file not found: {self.schema_path}", file=sys.stderr)
                sys.exit(1)

            print(f"Creating database from schema: {self.schema_path}")
            with open(self.schema_path) as f:
                schema_sql = f.read()

            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            conn.executescript(schema_sql)
            conn.commit()
            print(f"Database created: {self.db_path}")
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

        # Remove leading/trailing hyphens (EXT-033b)
        title = title.strip('-')

        # Remove consecutive hyphens (EXT-033c)
        title = re.sub(r'-+', '-', title)

        # Truncate at 100 characters (EXT-033e)
        if len(title) > 100:
            title = title[:97] + '...'  # EXT-033f: ellipsis indicator

        return title

    def get_next_rule_id(self, rule_type):
        """Generate next sequential rule ID (EXT-030, EXT-030a, EXT-031, EXT-031a, EXT-032)."""
        # Map rule types to prefixes
        type_map = {
            'decisions': 'ADR',
            'constraints': 'CON',
            'invariants': 'INV'
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
                # Extract number from ID like "CON-00073"
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

        # EXT-031, EXT-031a: Format using template replacement
        rule_id = self.rule_id_format.replace('{TYPE}', prefix).replace(
            '{NNNNN}', str(seq).zfill(self.rule_id_padding)
        )

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
        """Process single chatlog in atomic transaction (EXT-050, EXT-051, EXT-052, EXT-053)."""
        try:
            # Load chatlog
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)

            # EXT-010: Validate schema version
            chatlog_version = chatlog.get('schema_version', '')
            if chatlog_version != self.expected_schema_version:
                print(f"  ERROR: Incompatible schema version {chatlog_version}, expected {self.expected_schema_version}")
                return False  # EXT-011, EXT-013: Skip without marking processed

            # EXT-012: Validate required fields
            required_fields = ['chatlog_id', 'timestamp', 'schema_version', 'rules']
            for field in required_fields:
                if field not in chatlog:
                    print(f"  ERROR: Missing required field '{field}'")
                    return False  # EXT-013: Skip without marking processed

            # EXT-012a: Validate rules is a dict
            if not isinstance(chatlog['rules'], dict):
                print(f"  ERROR: rules field is not a dict (found {type(chatlog['rules']).__name__})")
                return False

            # EXT-065: Load reusability scope map
            session_context = chatlog.get('session_context', {})
            reusability_scope_map = session_context.get('reusability_scope', {})

            chatlog_id = chatlog['chatlog_id']
            chatlog_metadata = chatlog.get('metadata', {})

            rules_to_insert = []

            # EXT-020: Process each category
            categories = ['decisions', 'constraints', 'invariants']
            prefix_map = {'decisions': 'ADR', 'constraints': 'CON', 'invariants': 'INV'}

            for category_key in categories:
                category_rules = chatlog['rules'].get(category_key, [])

                # EXT-012b: Validate category is a list
                if not isinstance(category_rules, list):
                    print(f"  WARNING: {category_key} is not a list, skipping category")
                    continue

                for rule_index, rule in enumerate(category_rules):
                    # Validate rule structure
                    if not isinstance(rule, dict):
                        print(f"  WARNING: {category_key}[{rule_index}] is not a dict, skipping")
                        continue

                    # EXT-021, EXT-022: Filter by confidence threshold
                    confidence = rule.get('confidence', 0.0)
                    if confidence < 0.5:
                        self.stats['rules_filtered'] += 1
                        if self.verbose:
                            print(f"  Filtered {category_key}[{rule_index}]: confidence {confidence} < 0.5")
                        continue

                    # Generate rule ID (EXT-030, EXT-030a)
                    rule_id = self.get_next_rule_id(category_key)

                    # EXT-033: Normalize title from topic
                    topic = rule.get('topic', 'untitled')
                    title = self.normalize_title(topic)

                    # EXT-034: Description from rationale
                    description = rule.get('rationale', '')

                    # Build metadata (EXT-066)
                    metadata = {
                        'reusability_scope': self.resolve_reusability_scope(
                            category_key, rule_index, reusability_scope_map
                        )
                    }

                    # Add category-specific metadata
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
                                'created_at': chatlog_metadata.get('captured_at', chatlog['timestamp'])
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
                                'created_at': chatlog_metadata.get('captured_at', chatlog['timestamp'])
                            }
                            for ref in rule['implementation_refs']
                        ]

                    # EXT-091, EXT-092: Assign salience BEFORE json.dumps
                    salience, metadata = self.assign_default_salience(prefix_map[category_key], metadata)

                    # Build rule record
                    rule_record = {
                        'id': rule_id,
                        'type': prefix_map[category_key],
                        'title': title,
                        'description': description,
                        'domain': rule.get('domain', ''),
                        'confidence': confidence,
                        'tags_state': 'needs_tags',  # EXT-041
                        'lifecycle': 'active',
                        'tags': '[]',  # EXT-040: Empty tags array
                        'chatlog_id': chatlog_id,  # EXT-060: Provenance
                        'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                        'metadata': json.dumps(metadata),
                        'salience': salience
                    }

                    rules_to_insert.append(rule_record)

                    # Update statistics
                    self.stats['rules_extracted'] += 1
                    self.stats['rules_by_type'][prefix_map[category_key]] += 1
                    self.stats['total_confidence'] += confidence

            # EXT-023: Process even if no qualifying rules
            if not rules_to_insert:
                print(f"  No qualifying rules found (all filtered or empty)")

            if self.dry_run:
                print(f"  DRY RUN: Would insert {len(rules_to_insert)} rules")
                return True

            # EXT-050, EXT-050a: Process in transaction (no explicit BEGIN)
            try:
                # Insert chatlog record (EXT-008, EXT-060)
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
                        """INSERT INTO rules (id, type, title, description, domain, confidence,
                                             tags_state, lifecycle, tags, chatlog_id, created_at, metadata, salience)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            rule_record['id'],
                            rule_record['type'],
                            rule_record['title'],
                            rule_record['description'],
                            rule_record['domain'],
                            rule_record['confidence'],
                            rule_record['tags_state'],
                            rule_record['lifecycle'],
                            rule_record['tags'],
                            rule_record['chatlog_id'],
                            rule_record['created_at'],
                            rule_record['metadata'],
                            rule_record['salience']
                        )
                    )

                # EXT-051: Commit transaction
                self.conn.commit()

                # Update statistics
                self.stats['chatlogs_processed'] += 1

                return True

            except Exception as e:
                # EXT-052: Rollback on failure
                self.conn.rollback()
                # EXT-053: Log and continue
                print(f"  ERROR: Transaction failed: {e}")
                return False

        except Exception as e:
            # EXT-053: Log and continue
            print(f"  ERROR: Failed to process chatlog: {e}")
            if self.verbose:
                import traceback
                traceback.print_exc()
            return False

    def run(self):
        """Process all unprocessed chatlogs (EXT-001, EXT-002, EXT-005)."""
        print("Chatlog Extraction Process")
        print("="*70)

        # EXT-002, EXT-004: Get unprocessed chatlogs in chronological order
        unprocessed = self.get_unprocessed_chatlogs()

        if not unprocessed:
            print("\nNo unprocessed chatlogs found.")
            return True  # EXT-005: Success when nothing to process

        print(f"\nFound {len(unprocessed)} unprocessed chatlog(s)")

        # Process each chatlog
        for i, chatlog_path in enumerate(unprocessed, 1):
            # EXT-070: Report per-chatlog progress
            print(f"\n[{i}/{len(unprocessed)}] Processing: {chatlog_path.name}")
            self.process_chatlog(chatlog_path)

        # EXT-071: Summary report
        print("\n" + "="*70)
        print("Extraction Summary")
        print("="*70)
        print(f"Chatlogs processed: {self.stats['chatlogs_processed']}")
        print(f"Rules extracted: {self.stats['rules_extracted']}")
        print(f"  Decisions (ADR): {self.stats['rules_by_type']['ADR']}")
        print(f"  Constraints (CON): {self.stats['rules_by_type']['CON']}")
        print(f"  Invariants (INV): {self.stats['rules_by_type']['INV']}")
        print(f"Rules filtered (confidence < 0.5): {self.stats['rules_filtered']}")

        if self.stats['rules_extracted'] > 0:
            avg_confidence = self.stats['total_confidence'] / self.stats['rules_extracted']
            print(f"Average confidence: {avg_confidence:.2f}")

        return True

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Transform chatlogs into database rules with validation and provenance (EXT-062)."""
    parser = argparse.ArgumentParser(
        description='Extract rules from chatlogs into database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without making changes'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed processing information'
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"ERROR: Failed to load configuration: {e}", file=sys.stderr)
        return 1  # EXT-072: Exit with failure code

    # Create and run extractor
    extractor = None
    try:
        extractor = ChatlogExtractor(config, dry_run=args.dry_run, verbose=args.verbose)
        success = extractor.run()
        return 0 if success else 1  # EXT-072: Success/failure exit codes

    except Exception as e:
        print(f"ERROR: Extraction failed: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1  # EXT-072: Exit with failure code

    finally:
        if extractor:
            extractor.close()


if __name__ == '__main__':
    sys.exit(main())
