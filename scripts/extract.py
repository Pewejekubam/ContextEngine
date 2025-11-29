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


def resolve_reusability_scope(category_key, rule_index, reusability_scope_map):
    """
    EXT-065, EXT-066, EXT-067: Resolve rule scope from chatlog references

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
    """ETL processor for chatlog to database extraction (EXT-001)"""

    def __init__(self, config):
        """Initialize extractor with configuration"""
        self.config = config

        # EXT-014: Database path from config['structure']['database_path']
        self.db_path = BASE_DIR / config['structure']['database_path']

        # EXT-015: Chatlogs directory from config['structure']['chatlogs_dir']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']

        # EXT-016: Schema directory from config['structure']['schema_dir']
        self.schema_path = BASE_DIR / config['structure']['schema_dir'] / 'schema.sql'

        # EXT-010: Schema version from config['behavior']['chatlog_schema_version']
        self.expected_schema_version = config['behavior']['chatlog_schema_version']

        # EXT-031: Rule ID format configuration
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: In-memory ID counters per type
        self.id_counters = {}

        # Statistics (EXT-071)
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'filtered_rules': 0
        }

        # Database connection
        self.conn = None

        # EXT-093: Load salience defaults
        self.salience_defaults = self.load_salience_config()

    def load_salience_config(self):
        """EXT-091: Load salience defaults from config"""
        return self.config.get('salience_defaults', {
            'INV': 0.8,
            'ADR': 0.7,
            'CON': 0.6,
            'PAT': 0.5
        })

    def verify_schema_version(self):
        """EXT-093: Verify schema version v1.2.0"""
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

    def ensure_database(self):
        """EXT-006, EXT-007: Create database from schema if it doesn't exist"""
        if not self.db_path.exists():
            print(f"Creating database at {self.db_path}")
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Read schema
            if not self.schema_path.exists():
                print(f"ERROR: Schema file not found: {self.schema_path}", file=sys.stderr)
                sys.exit(1)

            with open(self.schema_path) as f:
                schema_sql = f.read()

            # Create database
            conn = sqlite3.connect(self.db_path)
            conn.executescript(schema_sql)
            conn.commit()
            conn.close()

    def connect_database(self):
        """Connect to database with row factory"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # EXT-093: Verify schema version
        self.verify_schema_version()

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
        """EXT-033: URL-safe normalization of topic to title"""
        # Convert to lowercase
        title = topic.lower()

        # EXT-033a: Replace non-alphanumeric with hyphens
        title = re.sub(r'[^a-z0-9]+', '-', title)

        # EXT-033b: Strip leading/trailing hyphens
        title = title.strip('-')

        # EXT-033c: Replace consecutive hyphens
        title = re.sub(r'-+', '-', title)

        # EXT-033e, EXT-033f: Truncate at 100 characters with ellipsis
        if len(title) > 100:
            title = title[:97] + '...'

        return title

    def get_next_rule_id(self, rule_type):
        """EXT-030, EXT-030a, EXT-031, EXT-031a: Generate next unique rule ID"""
        type_map = {
            'decision': 'ADR',
            'constraint': 'CON',
            'invariant': 'INV'
        }
        prefix = type_map[rule_type]

        # Initialize counter on first call for this type
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

        # EXT-031a: Format using string replacement
        rule_id = self.rule_id_format.replace('{TYPE}', prefix).replace(
            '{NNNNN}', str(seq).zfill(self.rule_id_padding)
        )

        return rule_id

    def assign_default_salience(self, rule_record, metadata_dict):
        """EXT-091, EXT-092: Assign default salience if not already set

        Args:
            rule_record: Dict with rule fields (will be modified)
            metadata_dict: Dict with metadata (will be modified)
        """
        # EXT-092: Skip if already assigned
        if 'salience_method' in metadata_dict:
            return

        # EXT-091: Assign default
        if rule_record.get('salience') is None:
            rule_type = rule_record['type']
            rule_record['salience'] = self.salience_defaults.get(rule_type, 0.5)
            metadata_dict['salience_method'] = 'default'

    def validate_chatlog(self, chatlog, chatlog_path):
        """EXT-010, EXT-011, EXT-012, EXT-012a, EXT-012b: Validate chatlog structure"""
        # EXT-010: Schema version check
        if chatlog.get('schema_version') != self.expected_schema_version:
            print(f"  ERROR: Incompatible schema version {chatlog.get('schema_version')}, expected {self.expected_schema_version}")
            return False

        # EXT-012: Required fields
        required_fields = ['chatlog_id', 'timestamp', 'rules']
        for field in required_fields:
            if field not in chatlog:
                print(f"  ERROR: Missing required field '{field}'")
                return False

        # EXT-012a: Validate rules is dict
        if not isinstance(chatlog['rules'], dict):
            print(f"  ERROR: 'rules' field must be dict, got {type(chatlog['rules'])}")
            return False

        # EXT-012b: Validate categories are lists
        for category in ['decisions', 'constraints', 'invariants']:
            if category in chatlog['rules'] and not isinstance(chatlog['rules'][category], list):
                print(f"  ERROR: rules['{category}'] must be list, got {type(chatlog['rules'][category])}")
                return False

        return True

    def process_chatlog(self, chatlog_path):
        """EXT-050: Process single chatlog in atomic transaction"""
        print(f"\nProcessing: {chatlog_path.name}")

        # Load chatlog
        try:
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)
        except Exception as e:
            print(f"  ERROR: Failed to load YAML: {e}")
            return False

        # EXT-013: Validate before processing
        if not self.validate_chatlog(chatlog, chatlog_path):
            print(f"  SKIPPED: Validation failed")
            return False

        # EXT-065: Load reusability scope map
        session_context = chatlog.get('session_context', {})
        reusability_scope_map = session_context.get('reusability_scope', {})

        try:
            # EXT-050a: No explicit BEGIN - sqlite3 auto-starts transactions

            # Insert chatlog record
            chatlog_id = chatlog['chatlog_id']
            timestamp = chatlog['timestamp']
            schema_version = chatlog['schema_version']
            agent = chatlog.get('agent', 'unknown')
            processed_at = datetime.now(UTC).isoformat().replace('+00:00', 'Z')

            self.conn.execute(
                """INSERT INTO chatlogs (chatlog_id, filename, timestamp, schema_version, agent, processed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (chatlog_id, chatlog_path.name, timestamp, schema_version, agent, processed_at)
            )

            # EXT-020: Extract rules from categories
            rules_extracted = 0
            categories = {
                'decisions': 'decision',
                'constraints': 'constraint',
                'invariants': 'invariant'
            }

            for category_key, rule_type in categories.items():
                category_rules = chatlog['rules'].get(category_key, [])

                for rule_index, rule_data in enumerate(category_rules):
                    # EXT-021: Confidence threshold
                    confidence = rule_data.get('confidence', 1.0)
                    if confidence < 0.5:
                        print(f"  Filtered: {rule_type} '{rule_data.get('topic', 'unknown')}' (confidence {confidence})")
                        self.stats['filtered_rules'] += 1
                        continue

                    # Generate rule ID
                    rule_id = self.get_next_rule_id(rule_type)

                    # EXT-033: Normalize title
                    title = self.normalize_title(rule_data.get('topic', 'untitled'))

                    # EXT-034: Description from rationale
                    description = rule_data.get('rationale', '')

                    # EXT-066: Resolve reusability scope
                    scope = resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                    # Build metadata
                    metadata = {'reusability_scope': scope}

                    # Category-specific metadata
                    if category_key == 'decisions':
                        metadata['alternatives_rejected'] = rule_data.get('alternatives_rejected', [])
                    elif category_key == 'constraints':
                        metadata['validation_method'] = rule_data.get('validation_method', '')

                    # RREL-004: Extract relationships
                    if 'relationships' in rule_data:
                        metadata['relationships'] = [
                            {
                                'type': rel['type'],
                                'target': rel['target'],
                                'rationale': rel['rationale'],
                                'created_at': timestamp
                            }
                            for rel in rule_data['relationships']
                        ]

                    # RREL-008a: Extract implementation references
                    if 'implementation_refs' in rule_data:
                        metadata['implementation_refs'] = [
                            {
                                'type': ref['type'],
                                'file': ref['file'],
                                'lines': ref.get('lines'),
                                'role_description': ref['role_description'],
                                'created_at': timestamp
                            }
                            for ref in rule_data['implementation_refs']
                        ]

                    # Prepare rule record
                    rule_record = {
                        'id': rule_id,
                        'type': type_map_db[rule_type],
                        'title': title,
                        'description': description,
                        'domain': rule_data.get('domain', ''),
                        'confidence': confidence,
                        'tags_state': 'needs_tags',  # EXT-041
                        'lifecycle': 'active',
                        'tags': '[]',  # EXT-040
                        'chatlog_id': chatlog_id,
                        'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                        'salience': rule_data.get('salience')  # May be None
                    }

                    # EXT-091, EXT-092: Assign default salience
                    self.assign_default_salience(rule_record, metadata)

                    # Insert rule
                    self.conn.execute(
                        """INSERT INTO rules
                           (id, type, title, description, domain, confidence, tags_state, lifecycle,
                            tags, chatlog_id, created_at, metadata, salience)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (rule_record['id'], rule_record['type'], rule_record['title'],
                         rule_record['description'], rule_record['domain'], rule_record['confidence'],
                         rule_record['tags_state'], rule_record['lifecycle'], rule_record['tags'],
                         rule_record['chatlog_id'], rule_record['created_at'],
                         json.dumps(metadata) if metadata else None, rule_record['salience'])
                    )

                    rules_extracted += 1
                    self.stats['rules_by_type'][type_map_db[rule_type]] += 1
                    self.stats['total_confidence'] += confidence

            # EXT-051: Commit transaction
            self.conn.commit()

            # EXT-070: Report progress
            print(f"  Extracted {rules_extracted} rules")
            self.stats['total_chatlogs'] += 1
            self.stats['total_rules'] += rules_extracted

            return True

        except Exception as e:
            # EXT-052, EXT-053: Rollback and continue
            self.conn.rollback()
            print(f"  ERROR: Transaction failed: {e}")
            return False

    def run(self):
        """EXT-001: Main extraction process"""
        print("Starting chatlog extraction...")

        # EXT-006: Ensure database exists
        self.ensure_database()

        # Connect to database
        self.connect_database()

        # EXT-004: Get unprocessed chatlogs
        unprocessed = self.get_unprocessed_chatlogs()
        print(f"\nFound {len(unprocessed)} unprocessed chatlog(s)")

        if not unprocessed:
            print("No unprocessed chatlogs found.")
            return 0

        # Process each chatlog
        for chatlog_path in unprocessed:
            self.process_chatlog(chatlog_path)

        # EXT-071: Summary
        print("\n" + "="*70)
        print("Extraction Summary:")
        print(f"  Total chatlogs processed: {self.stats['total_chatlogs']}")
        print(f"  Total rules extracted: {self.stats['total_rules']}")
        print(f"  Rules by type:")
        for rule_type, count in self.stats['rules_by_type'].items():
            print(f"    {rule_type}: {count}")
        if self.stats['total_rules'] > 0:
            avg_conf = self.stats['total_confidence'] / self.stats['total_rules']
            print(f"  Average confidence: {avg_conf:.2f}")
        if self.stats['filtered_rules'] > 0:
            print(f"  Filtered rules (confidence < 0.5): {self.stats['filtered_rules']}")

        return 0


# Type mapping for database
type_map_db = {
    'decision': 'ADR',
    'constraint': 'CON',
    'invariant': 'INV'
}


def main():
    """EXT-001: Transform chatlogs into database rules with validation and provenance"""
    print("Context Engine - Extract Module")
    print("="*70)

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Run extraction
    extractor = ChatlogExtractor(config)
    result = extractor.run()

    # EXT-072: Exit with success (0) or failure (1)
    return result


if __name__ == '__main__':
    sys.exit(main())
