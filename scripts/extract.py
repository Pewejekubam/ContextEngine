#!/usr/bin/env python3
"""
Chatlog to database ETL script with pure extraction (no tag normalization)

Implements constraints: EXT-001 through EXT-093
Generated from: specs/modules/runtime-script-etl-extract-v1.2.4.yaml
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
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


# ============================================================================
# CHATLOG EXTRACTOR CLASS
# ============================================================================

class ChatlogExtractor:
    """ETL processor for chatlog to database transformation (EXT-001)."""

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

        # EXT-031, EXT-031a: Rule ID format from config
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: In-memory ID counters per type
        self.id_counters = {}

        # EXT-091, EXT-093: Load salience defaults
        self.salience_defaults = self.load_salience_config()

        # Database connection
        self.conn = None

        # Statistics tracking (EXT-071)
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'filtered_rules': 0,
            'skipped_chatlogs': 0
        }

    def load_salience_config(self):
        """Load salience defaults from build-constants.yaml (EXT-091)."""
        # Try to load from build-constants.yaml
        build_constants_path = BASE_DIR / '..' / '..' / 'build' / 'config' / 'build-constants.yaml'

        # Fallback to hardcoded defaults if file not accessible
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
                        defaults = build_config['salience_defaults']
        except Exception:
            # Use hardcoded defaults on any error
            pass

        return defaults

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

    def initialize_database(self):
        """Initialize database connection and create from schema if needed (EXT-006, EXT-007)."""
        # Create parent directory if needed
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if database exists
        db_exists = self.db_path.exists()

        # Connect to database
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # EXT-006: Create from schema if database does not exist
        if not db_exists:
            print(f"Creating database from schema: {self.schema_path}")
            with open(self.schema_path) as f:
                schema_sql = f.read()
            self.conn.executescript(schema_sql)
            self.conn.commit()

        # EXT-093: Verify schema version
        self.verify_schema_version()

    def get_unprocessed_chatlogs(self):
        """Find chatlogs not yet processed (EXT-002, EXT-004, EXT-004a, EXT-061).

        EXT-061: Chatlog processing state prevents duplicate rule creation.
        By checking processed_at in chatlogs table, we ensure each chatlog
        is processed exactly once (EXT-009: after database loss, reprocess all).
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

            # Unprocessed if: not in database OR processed_at is NULL (EXT-003, EXT-004)
            # EXT-009: Database loss means no rows, triggering reprocessing
            if not row or row['processed_at'] is None:
                unprocessed.append(chatlog_file)

        return unprocessed

    def get_next_rule_id(self, rule_type):
        """Generate next rule ID with in-transaction tracking (EXT-030, EXT-030a, EXT-031, EXT-031a, EXT-032)."""
        # Map category to type prefix
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
                # Extract number from existing ID (e.g., "ADR-00042" -> 42)
                match = re.search(r'\d+', row['id'])
                seq = int(match.group()) + 1 if match else 1
            else:
                seq = 1
            self.id_counters[prefix] = seq

        # Allocate next ID from counter
        seq = self.id_counters[prefix]
        self.id_counters[prefix] += 1

        # EXT-031, EXT-031a: Format using template string replacement
        rule_id = self.rule_id_format.replace('{TYPE}', prefix).replace('{NNNNN}', str(seq).zfill(self.rule_id_padding))

        return rule_id, prefix

    def normalize_title(self, topic):
        """Normalize topic to URL-safe title (EXT-033, EXT-033a-f, EXT-033d)."""
        if not topic:
            return 'untitled'

        # Convert to lowercase and replace spaces/underscores with hyphens
        # EXT-033d: Preserve semantics by keeping alphanumeric characters
        title = topic.lower()
        title = re.sub(r'[_\s]+', '-', title)

        # EXT-033a: Keep only lowercase letters, digits, and hyphens
        title = re.sub(r'[^a-z0-9-]', '', title)

        # EXT-033c: Replace consecutive hyphens with single hyphen
        title = re.sub(r'-+', '-', title)

        # EXT-033b: Strip leading/trailing hyphens
        title = title.strip('-')

        # EXT-033e, EXT-033f: Truncate at 100 characters with ellipsis
        if len(title) > 100:
            title = title[:97] + '...'

        return title if title else 'untitled'

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

    def process_chatlog(self, chatlog_path):
        """Process single chatlog with transaction (EXT-050, EXT-051, EXT-052, EXT-053)."""
        try:
            # Load chatlog
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)

            # EXT-010: Validate schema version
            chatlog_version = chatlog.get('schema_version', '')
            if chatlog_version != self.expected_schema_version:
                print(f"  SKIPPED: Incompatible schema version {chatlog_version} (expected {self.expected_schema_version})")
                self.stats['skipped_chatlogs'] += 1
                return  # EXT-011, EXT-013: Skip without marking processed

            # EXT-012: Validate required fields
            required_fields = ['chatlog_id', 'timestamp', 'rules']
            for field in required_fields:
                if field not in chatlog:
                    print(f"  SKIPPED: Missing required field '{field}'")
                    self.stats['skipped_chatlogs'] += 1
                    return  # EXT-013: Skip without marking processed

            # EXT-012a: Validate rules is a dict
            rules_data = chatlog.get('rules', {})
            if not isinstance(rules_data, dict):
                print(f"  SKIPPED: 'rules' field is not a dict")
                self.stats['skipped_chatlogs'] += 1
                return

            # EXT-065: Load reusability scope map
            session_context = chatlog.get('session_context', {})
            reusability_scope_map = session_context.get('reusability_scope', {})

            # Prepare rule list
            extracted_rules = []

            # EXT-020: Process each category
            for category_key in ['decisions', 'constraints', 'invariants']:
                category_rules = rules_data.get(category_key, [])

                # EXT-012b: Validate category contains a list
                if not isinstance(category_rules, list):
                    print(f"  WARNING: '{category_key}' is not a list, skipping category")
                    continue

                # Process each rule in category with index (for scope resolution)
                for rule_index, rule in enumerate(category_rules):
                    # Validate rule structure
                    if not isinstance(rule, dict):
                        continue

                    topic = rule.get('topic', '')
                    rationale = rule.get('rationale', '')
                    confidence = rule.get('confidence', 0.0)
                    domain = rule.get('domain', '')

                    # EXT-021: Filter by confidence threshold
                    if confidence < 0.5:
                        print(f"    Filtered: {topic} (confidence {confidence})")
                        self.stats['filtered_rules'] += 1
                        continue  # EXT-022: Exclude from database

                    # EXT-030: Generate unique ID
                    rule_id, rule_type = self.get_next_rule_id(category_key)

                    # EXT-033: Normalize title
                    title = self.normalize_title(topic)

                    # EXT-034: Description from rationale
                    description = rationale

                    # EXT-066: Resolve reusability scope
                    scope = self.resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                    # Build metadata based on category
                    if category_key == 'decisions':
                        metadata = {
                            'reusability_scope': scope,
                            'alternatives_rejected': rule.get('alternatives_rejected', [])
                        }
                    elif category_key == 'constraints':
                        metadata = {
                            'reusability_scope': scope,
                            'validation_method': rule.get('validation_method', '')
                        }
                    else:  # invariants
                        metadata = {
                            'reusability_scope': scope
                        }

                    # Prepare rule record
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

                    # EXT-091, EXT-092: Assign default salience
                    self.assign_default_salience(rule_record)

                    extracted_rules.append(rule_record)
                    self.stats['total_rules'] += 1
                    self.stats['rules_by_type'][rule_type] += 1
                    self.stats['total_confidence'] += confidence

            # EXT-023: Chatlogs with no qualifying rules are still marked processed
            if not extracted_rules:
                print(f"  No qualifying rules (all filtered or missing)")

            # EXT-050a: Do not use explicit BEGIN - sqlite3 auto-starts transactions
            # EXT-051: Transaction includes chatlog record and rule records

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
            for rule_record in extracted_rules:
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
                        json.dumps(rule_record['metadata']) if rule_record['metadata'] else None
                    )
                )

            # EXT-050: Commit transaction
            self.conn.commit()

            # EXT-070: Report per-chatlog progress
            print(f"  Processed: {len(extracted_rules)} rules extracted")
            self.stats['total_chatlogs'] += 1

        except Exception as e:
            # EXT-052: Rollback on failure
            if self.conn:
                self.conn.rollback()
            # EXT-053: Log failure and continue
            print(f"  ERROR: Transaction failed: {e}")
            self.stats['skipped_chatlogs'] += 1

    def run(self):
        """Main extraction process (EXT-001, EXT-002, EXT-005)."""
        print("Chatlog to Database ETL - Pure Extraction")
        print("="*70)

        # Initialize database
        self.initialize_database()

        # EXT-002: Get unprocessed chatlogs in chronological order (sorted by filename)
        unprocessed = self.get_unprocessed_chatlogs()

        if not unprocessed:
            print("\nNo unprocessed chatlogs found.")
            print("\nExtraction complete: All chatlogs processed")
            return 0  # EXT-005: Success when no unprocessed remain

        print(f"\nFound {len(unprocessed)} unprocessed chatlog(s)")
        print()

        # Process each chatlog
        for chatlog_path in unprocessed:
            print(f"Processing: {chatlog_path.name}")
            self.process_chatlog(chatlog_path)

        # Close database
        if self.conn:
            self.conn.close()

        # EXT-071: Print summary
        print("\n" + "="*70)
        print("EXTRACTION SUMMARY")
        print("="*70)
        print(f"Total chatlogs processed: {self.stats['total_chatlogs']}")
        print(f"Chatlogs skipped (errors): {self.stats['skipped_chatlogs']}")
        print(f"Total rules extracted: {self.stats['total_rules']}")
        print(f"  Decisions (ADR): {self.stats['rules_by_type']['ADR']}")
        print(f"  Constraints (CON): {self.stats['rules_by_type']['CON']}")
        print(f"  Invariants (INV): {self.stats['rules_by_type']['INV']}")
        print(f"Rules filtered (low confidence): {self.stats['filtered_rules']}")

        if self.stats['total_rules'] > 0:
            avg_confidence = self.stats['total_confidence'] / self.stats['total_rules']
            print(f"Average confidence: {avg_confidence:.3f}")

        print()

        # EXT-072: Exit with success (0) or failure (1)
        return 0


def main():
    """Transform chatlogs into database rules with validation and provenance (EXT-062)."""
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Run extractor
    extractor = ChatlogExtractor(config)
    return extractor.run()


if __name__ == '__main__':
    sys.exit(main())
