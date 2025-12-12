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
# CHATLOG ETL EXTRACTOR
# ============================================================================

class ChatlogExtractor:
    """Transform chatlogs into database rules with validation and provenance."""

    def __init__(self, config):
        """Initialize extractor with configuration (EXT-014, EXT-015, EXT-016)."""
        self.config = config

        # EXT-014: Database path from config['structure']['database_path']
        self.db_path = BASE_DIR / config['structure']['database_path']

        # EXT-015: Chatlogs directory from config['structure']['chatlogs_dir']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']

        # EXT-016: Schema directory from config['structure']['schema_dir']
        self.schema_path = BASE_DIR / config['structure']['schema_dir'] / 'schema.sql'

        # EXT-010: Schema version from config['behavior']['chatlog_schema_version']
        self.expected_schema = config['behavior']['chatlog_schema_version']

        # EXT-031, EXT-031a: Rule ID format configuration
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: In-memory counter for ID generation within transaction
        self.id_counters = {}  # {type: next_available_seq}

        # EXT-091: Salience defaults (SAL-009)
        self.salience_defaults = self.load_salience_config()

        # Database connection
        self.conn = None

        # Statistics tracking
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'filtered_rules': 0
        }

    def load_salience_config(self):
        """Load salience defaults from build-constants.yaml (SAL-009)."""
        try:
            constants_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'
            if constants_path.exists():
                with open(constants_path) as f:
                    constants = yaml.safe_load(f)
                    return constants.get('salience_defaults', {
                        'INV': 0.8,
                        'ADR': 0.7,
                        'CON': 0.6,
                        'PAT': 0.5
                    })
        except Exception:
            pass

        # Fallback to hardcoded defaults
        return {
            'INV': 0.8,
            'ADR': 0.7,
            'CON': 0.6,
            'PAT': 0.5
        }

    def verify_schema_version(self):
        """Verify schema version v1.2.0 (EXT-093, SAL-012)."""
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

    def initialize_database(self):
        """Initialize database connection and create schema if needed (EXT-006, EXT-007)."""
        db_exists = self.db_path.exists()

        # Create parent directory if needed
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to database
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

        # EXT-006: Create database from schema if not exists
        if not db_exists:
            print(f"Creating new database from schema: {self.schema_path}")
            if not self.schema_path.exists():
                print(f"ERROR: Schema file not found: {self.schema_path}", file=sys.stderr)
                sys.exit(1)

            with open(self.schema_path) as f:
                schema_sql = f.read()
            self.conn.executescript(schema_sql)
            self.conn.commit()

        # EXT-093: Verify schema version (SAL-012)
        self.verify_schema_version()

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
        """Create URL-safe normalized title from topic (EXT-033, EXT-033a-f)."""
        # Convert to lowercase
        title = topic.lower()

        # Replace non-alphanumeric with hyphens
        title = re.sub(r'[^a-z0-9]+', '-', title)

        # Remove leading/trailing hyphens (EXT-033b)
        title = title.strip('-')

        # Collapse consecutive hyphens (EXT-033c)
        title = re.sub(r'-+', '-', title)

        # EXT-033e, EXT-033f: Truncate at 100 characters
        if len(title) > 100:
            title = title[:97] + '...'

        return title

    def get_next_rule_id(self, rule_type):
        """Generate next rule ID with in-transaction tracking (EXT-030, EXT-030a, EXT-031, EXT-031a)."""
        # Map category to type prefix
        type_map = {
            'decisions': 'ADR',
            'constraints': 'CON',
            'invariants': 'INV'
        }
        prefix = type_map.get(rule_type, 'ADR')

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

        # EXT-031a: Format using placeholder replacement (not .format())
        rule_id = self.rule_id_format.replace('{TYPE}', prefix).replace(
            '{NNNNN}', str(seq).zfill(self.rule_id_padding)
        )

        return rule_id, prefix

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
        """Process a single chatlog (EXT-050 through EXT-053, EXT-060, EXT-061)."""
        print(f"\nProcessing: {chatlog_path.name}")

        # Load chatlog
        try:
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)
        except Exception as e:
            print(f"  ERROR: Failed to load chatlog: {e}")
            return False  # EXT-013: Not marked as processed

        # EXT-010: Validate schema version
        chatlog_schema = chatlog.get('schema_version', '')
        if chatlog_schema != self.expected_schema:
            print(f"  ERROR: Schema version mismatch. Expected {self.expected_schema}, got {chatlog_schema}")
            return False  # EXT-011, EXT-013: Skip and don't mark as processed

        # EXT-012: Validate required fields
        required_fields = ['chatlog_id', 'timestamp', 'rules']
        for field in required_fields:
            if field not in chatlog:
                print(f"  ERROR: Missing required field: {field}")
                return False  # EXT-013

        # EXT-012a: Validate rules field is a dict
        rules = chatlog.get('rules', {})
        if not isinstance(rules, dict):
            print(f"  ERROR: rules field must be a dict, got {type(rules).__name__}")
            return False

        # Load reusability scope map (EXT-065)
        session_context = chatlog.get('session_context', {})
        reusability_scope_map = session_context.get('reusability_scope', {})

        # Extract metadata for relationships
        chatlog_metadata = chatlog.get('metadata', {})
        captured_at = chatlog_metadata.get('captured_at', chatlog.get('timestamp'))

        # Prepare to track rules
        rules_extracted = []

        # EXT-020: Process each category
        for category_key in ['decisions', 'constraints', 'invariants']:
            category_rules = rules.get(category_key, [])

            # EXT-012b: Validate category contains a list
            if not isinstance(category_rules, list):
                print(f"  WARNING: {category_key} is not a list, skipping category")
                continue

            # Process each rule in category (with index for reusability scope)
            for rule_index, rule in enumerate(category_rules):
                # Skip if not a dict
                if not isinstance(rule, dict):
                    print(f"  WARNING: Rule in {category_key}[{rule_index}] is not a dict, skipping")
                    continue

                # Validate required rule fields
                if 'topic' not in rule or 'rationale' not in rule:
                    print(f"  WARNING: Rule in {category_key}[{rule_index}] missing topic/rationale, skipping")
                    continue

                # EXT-021: Filter by confidence threshold
                confidence = rule.get('confidence', 0.0)
                if confidence < 0.5:
                    print(f"  FILTERED: {category_key}[{rule_index}] - confidence {confidence} < 0.5")
                    self.stats['filtered_rules'] += 1
                    continue  # EXT-022

                # Generate rule ID
                rule_id, rule_type = self.get_next_rule_id(category_key)

                # EXT-033: Normalize title
                title = self.normalize_title(rule.get('topic', ''))

                # EXT-034: Description from rationale
                description = rule.get('rationale', '')

                # EXT-066: Resolve reusability scope
                scope = self.resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                # Build metadata based on category
                metadata = {'reusability_scope': scope}

                if category_key == 'decisions':
                    metadata['alternatives_rejected'] = rule.get('alternatives_rejected', [])
                elif category_key == 'constraints':
                    metadata['validation_method'] = rule.get('validation_method', '')

                # RREL-004: Extract relationships
                if 'relationships' in rule:
                    metadata['relationships'] = [
                        {
                            'type': rel.get('type', ''),
                            'target': rel.get('target', ''),
                            'rationale': rel.get('rationale', ''),
                            'created_at': captured_at
                        }
                        for rel in rule.get('relationships', [])
                    ]

                # RREL-008a: Extract implementation references
                if 'implementation_refs' in rule:
                    metadata['implementation_refs'] = [
                        {
                            'type': ref.get('type', ''),
                            'file': ref.get('file', ''),
                            'lines': ref.get('lines'),
                            'role_description': ref.get('role_description', ''),
                            'created_at': captured_at
                        }
                        for ref in rule.get('implementation_refs', [])
                    ]

                # EXT-091, EXT-092: Assign salience BEFORE json.dumps
                salience, metadata = self.assign_default_salience(rule_type, metadata)

                # Build rule record
                rule_record = {
                    'id': rule_id,
                    'type': rule_type,
                    'title': title,
                    'description': description,
                    'domain': rule.get('domain'),
                    'confidence': confidence,
                    'salience': salience,
                    'tags_state': 'needs_tags',  # EXT-041
                    'lifecycle': 'active',
                    'tags': '[]',  # EXT-040: Empty tags array
                    'chatlog_id': chatlog['chatlog_id'],
                    'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                    'metadata': json.dumps(metadata)
                }

                rules_extracted.append(rule_record)

                # Update statistics
                self.stats['total_rules'] += 1
                self.stats['rules_by_type'][rule_type] += 1
                self.stats['total_confidence'] += confidence

        # EXT-023: Mark processed even if no qualifying rules
        if not rules_extracted:
            print(f"  No qualifying rules extracted (all filtered or invalid)")
        else:
            print(f"  Extracted {len(rules_extracted)} rules")

        # EXT-050: Atomic transaction (sqlite3 auto-starts, no explicit BEGIN)
        try:
            # Insert chatlog record (EXT-008, EXT-060)
            self.conn.execute(
                """
                INSERT INTO chatlogs (chatlog_id, filename, timestamp, schema_version, agent, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
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
            for rule_record in rules_extracted:
                self.conn.execute(
                    """
                    INSERT INTO rules (
                        id, type, title, description, domain, confidence, salience,
                        tags_state, lifecycle, tags, chatlog_id, created_at, metadata
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
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
            print(f"  SUCCESS: Committed {len(rules_extracted)} rules")
            self.stats['total_chatlogs'] += 1
            return True

        except Exception as e:
            # EXT-052: Rollback on failure
            self.conn.rollback()
            print(f"  ERROR: Transaction failed, rolled back: {e}")
            return False  # EXT-053: Log and continue

    def run(self):
        """Main extraction workflow (EXT-001 through EXT-005, EXT-070, EXT-071)."""
        print("Starting chatlog extraction...")

        # Initialize database
        self.initialize_database()

        # EXT-002, EXT-004: Get unprocessed chatlogs in chronological order
        unprocessed = self.get_unprocessed_chatlogs()

        if not unprocessed:
            print("\nNo unprocessed chatlogs found.")
            print("Extraction complete - nothing to do.")
            return True  # EXT-005

        print(f"\nFound {len(unprocessed)} unprocessed chatlog(s)")

        # Process each chatlog
        success_count = 0
        for chatlog_path in unprocessed:
            if self.process_chatlog(chatlog_path):
                success_count += 1

        # EXT-071: Summary report
        print("\n" + "="*70)
        print("EXTRACTION SUMMARY")
        print("="*70)
        print(f"Chatlogs processed: {self.stats['total_chatlogs']}/{len(unprocessed)}")
        print(f"Total rules extracted: {self.stats['total_rules']}")
        print(f"  Decisions (ADR): {self.stats['rules_by_type']['ADR']}")
        print(f"  Constraints (CON): {self.stats['rules_by_type']['CON']}")
        print(f"  Invariants (INV): {self.stats['rules_by_type']['INV']}")
        if self.stats['filtered_rules'] > 0:
            print(f"Rules filtered (confidence < 0.5): {self.stats['filtered_rules']}")
        if self.stats['total_rules'] > 0:
            avg_confidence = self.stats['total_confidence'] / self.stats['total_rules']
            print(f"Average confidence: {avg_confidence:.2f}")
        print("="*70)

        return success_count == len(unprocessed)


def main():
    """Transform chatlogs into database rules with validation and provenance"""
    print("Context Engine - Chatlog ETL Extract v1.4.1")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"ERROR: Failed to load configuration: {e}", file=sys.stderr)
        return 1  # EXT-072

    # Run extraction
    extractor = ChatlogExtractor(config)
    success = extractor.run()

    # EXT-072: Exit with appropriate code
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
