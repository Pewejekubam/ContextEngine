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
    """Load deployment configuration."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


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
    """ETL processor for chatlog to database extraction."""

    def __init__(self, config):
        """Initialize extractor with configuration."""
        self.config = config

        # EXT-014: Database path from config['structure']['database_path']
        self.db_path = BASE_DIR / config['structure']['database_path']

        # EXT-015: Chatlogs directory from config['structure']['chatlogs_dir']
        self.chatlogs_dir = BASE_DIR / config['structure']['chatlogs_dir']

        # EXT-016: Schema directory from config['structure']['schema_dir']
        self.schema_path = BASE_DIR / config['structure']['schema_dir'] / 'schema.sql'

        # EXT-010: Chatlog schema version from config['behavior']['chatlog_schema_version']
        self.expected_schema_version = config['behavior']['chatlog_schema_version']

        # EXT-031: Rule ID format and padding
        self.rule_id_format = config['behavior']['rule_id_format']
        self.rule_id_padding = config['behavior']['rule_id_padding']

        # EXT-030a: In-memory ID counter per type
        self.id_counters = {}

        # Initialize database connection
        self.conn = None
        self.initialize_database()

        # EXT-093: Load salience config and verify schema
        self.salience_defaults = self.load_salience_config()
        self.verify_schema_version()

        # Statistics
        self.stats = {
            'total_chatlogs': 0,
            'total_rules': 0,
            'rules_by_type': {'ADR': 0, 'CON': 0, 'INV': 0},
            'total_confidence': 0.0,
            'rules_count': 0,
            'skipped_chatlogs': 0,
            'filtered_rules': 0
        }

    def load_salience_config(self):
        """Load salience defaults from build-constants.yaml (EXT-091)."""
        # Try to load from build-constants.yaml
        constants_path = Path(__file__).parent.parent.parent / 'build' / 'config' / 'build-constants.yaml'

        if constants_path.exists():
            try:
                with open(constants_path) as f:
                    constants = yaml.safe_load(f)
                    return constants.get('salience_defaults', {
                        'INV': 0.8,
                        'ADR': 0.7,
                        'CON': 0.6,
                        'PAT': 0.5
                    })
            except Exception as e:
                print(f"  Warning: Failed to load salience config: {e}")

        # Fallback to hardcoded defaults
        return {
            'INV': 0.8,
            'ADR': 0.7,
            'CON': 0.6,
            'PAT': 0.5
        }

    def verify_schema_version(self):
        """Verify schema version v1.2.0 (EXT-093)."""
        try:
            # Query key-value table: schema_metadata(key, value, updated_at)
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
        """EXT-006: Create database from schema if it doesn't exist."""
        db_exists = self.db_path.exists()

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to database
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        if not db_exists:
            # EXT-007: Load schema from schema.sql
            if not self.schema_path.exists():
                print(f"ERROR: Schema file not found: {self.schema_path}", file=sys.stderr)
                sys.exit(1)

            with open(self.schema_path) as f:
                schema_sql = f.read()

            self.conn.executescript(schema_sql)
            print(f"  Created database from schema: {self.db_path}")

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
        """
        EXT-033: Normalize topic to URL-safe title
        EXT-033a-f: Various title normalization rules
        """
        if not topic:
            return "untitled"

        # Convert to lowercase
        title = topic.lower()

        # Replace non-alphanumeric with hyphens
        title = re.sub(r'[^a-z0-9]+', '-', title)

        # EXT-033b: Remove leading/trailing hyphens
        title = title.strip('-')

        # EXT-033c: Collapse consecutive hyphens
        title = re.sub(r'-+', '-', title)

        # EXT-033e, EXT-033f: Truncate at 100 characters with ellipsis
        if len(title) > 100:
            title = title[:97] + '...'

        return title or "untitled"

    def get_next_rule_id(self, rule_type):
        """
        EXT-030, EXT-030a, EXT-031, EXT-031a: Generate next sequential rule ID

        Args:
            rule_type: 'decisions', 'constraints', or 'invariants'

        Returns:
            str: Formatted rule ID (e.g., 'ADR-00042')
        """
        # Map rule type to prefix
        prefix_map = {
            'decisions': 'ADR',
            'constraints': 'CON',
            'invariants': 'INV'
        }
        prefix = prefix_map[rule_type]

        # EXT-030a: Initialize counter on first call for this type
        if prefix not in self.id_counters:
            # Query database for current max
            cursor = self.conn.execute(
                "SELECT id FROM rules WHERE type = ? ORDER BY id DESC LIMIT 1",
                (prefix,)
            )
            row = cursor.fetchone()
            if row:
                # Extract numeric portion from ID (e.g., 'ADR-00042' -> 42)
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

        # EXT-031, EXT-031a: Format using template with string replacement
        rule_id = self.rule_id_format.replace('{TYPE}', prefix)
        rule_id = rule_id.replace('{NNNNN}', str(seq).zfill(self.rule_id_padding))

        return rule_id

    def assign_default_salience(self, rule_type, metadata):
        """
        Assign default salience, returning (salience, updated_metadata) tuple (EXT-091, EXT-092).

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
        """
        Process a single chatlog file (EXT-050, EXT-051, EXT-052, EXT-053).

        Returns:
            tuple: (success: bool, rules_extracted: int)
        """
        print(f"\n  Processing: {chatlog_path.name}")

        try:
            # Load chatlog YAML
            with open(chatlog_path) as f:
                chatlog = yaml.safe_load(f)

            # EXT-010: Validate schema version
            schema_version = chatlog.get('schema_version')
            if schema_version != self.expected_schema_version:
                print(f"    ERROR: Schema version mismatch. Expected {self.expected_schema_version}, found {schema_version}")
                self.stats['skipped_chatlogs'] += 1
                return (False, 0)

            # EXT-012: Validate required fields
            required_fields = ['chatlog_id', 'timestamp', 'rules']
            for field in required_fields:
                if field not in chatlog:
                    print(f"    ERROR: Missing required field: {field}")
                    self.stats['skipped_chatlogs'] += 1
                    return (False, 0)

            # EXT-012a: Validate rules field is a dict
            rules = chatlog['rules']
            if not isinstance(rules, dict):
                print(f"    ERROR: rules field must be dict, found {type(rules).__name__}")
                self.stats['skipped_chatlogs'] += 1
                return (False, 0)

            # EXT-065: Load reusability scope map
            session_context = chatlog.get('session_context', {})
            reusability_scope_map = session_context.get('reusability_scope', {})

            # EXT-050a: No explicit BEGIN - sqlite3 auto-starts transactions
            # Process rules by category
            rules_extracted = 0

            for category_key in ['decisions', 'constraints', 'invariants']:
                category_rules = rules.get(category_key, [])

                # EXT-012b: Validate category is a list
                if not isinstance(category_rules, list):
                    print(f"    WARNING: {category_key} must be list, found {type(category_rules).__name__}, skipping")
                    continue

                # EXT-020: Extract rules from each category
                for rule_index, rule in enumerate(category_rules):
                    # Validate rule structure
                    if not isinstance(rule, dict):
                        print(f"    WARNING: {category_key}[{rule_index}] is not a dict, skipping")
                        continue

                    # EXT-021: Filter by confidence threshold
                    confidence = rule.get('confidence', 0.0)
                    if confidence < 0.5:
                        print(f"    FILTERED: {category_key}[{rule_index}] confidence {confidence} < 0.5")
                        self.stats['filtered_rules'] += 1
                        continue

                    # EXT-030: Generate rule ID
                    rule_id = self.get_next_rule_id(category_key)

                    # Map category to type prefix
                    prefix_map = {
                        'decisions': 'ADR',
                        'constraints': 'CON',
                        'invariants': 'INV'
                    }
                    rule_type = prefix_map[category_key]

                    # EXT-033: Normalize title
                    title = self.normalize_title(rule.get('topic', ''))

                    # EXT-034: Description from rationale
                    description = rule.get('rationale', '')

                    # EXT-066: Resolve reusability scope
                    scope = resolve_reusability_scope(category_key, rule_index, reusability_scope_map)

                    # Build metadata dict based on category
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

                    # RREL-004: Extract relationships if present
                    if 'relationships' in rule:
                        metadata['relationships'] = [
                            {
                                'type': rel.get('type', ''),
                                'target': rel.get('target', ''),
                                'rationale': rel.get('rationale', ''),
                                'created_at': chatlog.get('timestamp', '')
                            }
                            for rel in rule['relationships']
                        ]

                    # RREL-008a: Extract implementation_refs if present
                    if 'implementation_refs' in rule:
                        metadata['implementation_refs'] = [
                            {
                                'type': ref.get('type', ''),
                                'file': ref.get('file', ''),
                                'lines': ref.get('lines'),
                                'role_description': ref.get('role_description', ''),
                                'created_at': chatlog.get('timestamp', '')
                            }
                            for ref in rule['implementation_refs']
                        ]

                    # EXT-091, EXT-092: Assign salience BEFORE json.dumps
                    salience, metadata = self.assign_default_salience(rule_type, metadata)

                    # Now build rule_record with salience and serialized metadata
                    rule_record = {
                        'id': rule_id,
                        'type': rule_type,
                        'title': title,
                        'description': description,
                        'domain': rule.get('domain', ''),
                        'confidence': confidence,
                        'tags_state': 'needs_tags',  # EXT-041
                        'lifecycle': 'active',
                        'tags': '[]',  # EXT-040: Empty JSON array
                        'chatlog_id': chatlog['chatlog_id'],
                        'created_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                        'metadata': json.dumps(metadata),
                        'salience': salience
                    }

                    # Insert rule
                    self.conn.execute("""
                        INSERT INTO rules (id, type, title, description, domain, confidence,
                                         tags_state, lifecycle, tags, chatlog_id, created_at, metadata, salience)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
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
                    ))

                    rules_extracted += 1
                    self.stats['rules_by_type'][rule_type] += 1
                    self.stats['total_confidence'] += confidence
                    self.stats['rules_count'] += 1

            # EXT-023: Mark chatlog as processed even if no rules
            self.conn.execute("""
                INSERT INTO chatlogs (chatlog_id, filename, timestamp, schema_version, agent, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                chatlog['chatlog_id'],
                chatlog_path.name,
                chatlog['timestamp'],
                chatlog['schema_version'],
                chatlog.get('agent', 'unknown'),
                datetime.now(UTC).isoformat().replace('+00:00', 'Z')
            ))

            # EXT-051: Commit transaction
            self.conn.commit()

            print(f"    SUCCESS: Extracted {rules_extracted} rules")
            self.stats['total_rules'] += rules_extracted
            self.stats['total_chatlogs'] += 1

            return (True, rules_extracted)

        except Exception as e:
            # EXT-052: Rollback on failure
            self.conn.rollback()
            # EXT-053: Log failure and continue
            print(f"    ERROR: Transaction failed: {e}")
            import traceback
            traceback.print_exc()
            self.stats['skipped_chatlogs'] += 1
            return (False, 0)

    def run(self):
        """Main extraction workflow."""
        print("\nContext Engine - Chatlog Extraction")
        print("=" * 70)

        # EXT-002: Get unprocessed chatlogs in chronological order
        unprocessed = self.get_unprocessed_chatlogs()

        if not unprocessed:
            # EXT-005: Success when no unprocessed chatlogs
            print("\n  No unprocessed chatlogs found.")
            self.print_summary()
            return 0

        print(f"\n  Found {len(unprocessed)} unprocessed chatlog(s)")

        # Process each chatlog
        for chatlog_path in unprocessed:
            self.process_chatlog(chatlog_path)

        # EXT-071: Print summary
        self.print_summary()

        # EXT-072: Exit code
        return 0

    def print_summary(self):
        """EXT-071: Print extraction summary."""
        print("\n" + "=" * 70)
        print("EXTRACTION SUMMARY")
        print("=" * 70)
        print(f"  Total chatlogs processed: {self.stats['total_chatlogs']}")
        print(f"  Total rules extracted: {self.stats['total_rules']}")
        print(f"  Rules by type:")
        print(f"    Decisions (ADR): {self.stats['rules_by_type']['ADR']}")
        print(f"    Constraints (CON): {self.stats['rules_by_type']['CON']}")
        print(f"    Invariants (INV): {self.stats['rules_by_type']['INV']}")

        if self.stats['rules_count'] > 0:
            avg_confidence = self.stats['total_confidence'] / self.stats['rules_count']
            print(f"  Average confidence: {avg_confidence:.2f}")

        if self.stats['filtered_rules'] > 0:
            print(f"  Rules filtered (low confidence): {self.stats['filtered_rules']}")

        if self.stats['skipped_chatlogs'] > 0:
            print(f"  Chatlogs skipped (errors): {self.stats['skipped_chatlogs']}")

        print("=" * 70)

    def close(self):
        """Clean up database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Transform chatlogs into database rules with validation and provenance (EXT-001)."""
    try:
        # Load configuration
        config = load_config()

        # Create extractor
        extractor = ChatlogExtractor(config)

        # Run extraction
        exit_code = extractor.run()

        # Cleanup
        extractor.close()

        return exit_code

    except Exception as e:
        print(f"\nERROR: Extraction failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
