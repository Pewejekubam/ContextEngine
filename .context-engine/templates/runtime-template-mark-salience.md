# Mark Rule Salience (Manual Override)

> HITL workflow for overriding default salience values

<!-- TEMPLATE_METADATA
Template: runtime-template-mark-salience
Version: v1.0.0
Updated: 2025-11-01
Variables: []
Implements: Spec 27 SAL-004, SAL-005, SAL-006
END_TEMPLATE_METADATA -->

## Usage

```bash
/mark-salience <rule_id> <salience_value>
```

**Arguments:**
- `rule_id`: Rule identifier (e.g., ADR-00001, CON-00042, INV-00007, PAT-00003)
- `salience_value`: Priority score 0.0-1.0 (0.0=low, 1.0=critical)

**Examples:**
```bash
# Mark security invariant as critical
/mark-salience INV-00023 1.0

# Reduce priority of deprecated decision
/mark-salience ADR-00015 0.3

# Set standard priority for constraint
/mark-salience CON-00089 0.6
```

---

## Implementation

```bash
#!/bin/bash
set -e

# Validate arguments
if [ $# -ne 2 ]; then
    echo "ERROR: Usage: /mark-salience <rule_id> <salience_value>" >&2
    echo "Example: /mark-salience ADR-00001 0.9" >&2
    exit 1
fi

RULE_ID="$1"
SALIENCE="$2"

# Validate rule_id format (ADR-00001, CON-00042, etc.)
if ! echo "$RULE_ID" | grep -qE '^[A-Z]{3}-[0-9]{5}$'; then
    echo "ERROR: Invalid rule_id format: $RULE_ID" >&2
    echo "Expected format: XXX-NNNNN (e.g., ADR-00001)" >&2
    exit 1
fi

# Validate salience value (0.0-1.0)
if ! echo "$SALIENCE" | grep -qE '^(0(\.[0-9]+)?|1(\.0+)?)$'; then
    echo "ERROR: Invalid salience value: $SALIENCE" >&2
    echo "Expected range: 0.0 to 1.0" >&2
    exit 1
fi

# Additional numeric validation
if ! awk -v val="$SALIENCE" 'BEGIN { exit (val < 0.0 || val > 1.0) }'; then
    echo "ERROR: Salience value out of range: $SALIENCE" >&2
    echo "Expected: 0.0 <= value <= 1.0" >&2
    exit 1
fi

# Detect Context Engine installation
CONTEXT_ENGINE_HOME=""
if [ -f .context-engine/config/deployment.yaml ]; then
    CONTEXT_ENGINE_HOME=".context-engine"
elif [ -f "$HOME/.context-engine/config/deployment.yaml" ]; then
    CONTEXT_ENGINE_HOME="$HOME/.context-engine"
else
    echo "ERROR: Cannot find .context-engine installation" >&2
    echo "Looked in: .context-engine/config/, ~/.context-engine/config/" >&2
    exit 3
fi

# Read database path from deployment.yaml
DB_PATH=$(python3 -c "
import yaml, sys
try:
    with open('$CONTEXT_ENGINE_HOME/config/deployment.yaml') as f:
        config = yaml.safe_load(f)
    print(config['paths']['rules_database'])
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(3)
")

if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at: $DB_PATH" >&2
    exit 3
fi

# Verify schema version (MAJOR-02 fix: empty result handling)
SCHEMA_VERSION=$(sqlite3 "$DB_PATH" "SELECT value FROM schema_metadata WHERE key = 'schema_version';" 2>&1)

# Check if query succeeded and returned a value
if [ $? -ne 0 ] || [ -z "$SCHEMA_VERSION" ]; then
    echo "ERROR: Cannot determine schema version" >&2
    echo "ERROR: Database may be missing schema_metadata table" >&2
    echo "HINT: This database may predate Spec 25. Implement Schema Enhancement v1.2.0 first." >&2
    exit 4
fi

# Check version matches requirement
if [ "$SCHEMA_VERSION" != "1.2.0" ]; then
    echo "ERROR: Schema v1.2.0 required (found: $SCHEMA_VERSION)" >&2
    echo "ERROR: Salience column not available in schema v$SCHEMA_VERSION" >&2
    echo "HINT: Implement Spec 25 (Schema Enhancement) first" >&2
    exit 4
fi

# Check if rule exists
RULE_EXISTS=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM rules WHERE id = '$RULE_ID';")

if [ "$RULE_EXISTS" -eq 0 ]; then
    echo "ERROR: Rule $RULE_ID not found in database" >&2
    echo "Hint: Run 'sqlite3 $DB_PATH \"SELECT id, title FROM rules WHERE id LIKE '${RULE_ID:0:3}-%' ORDER BY id;\"' to see available rules" >&2
    exit 2
fi

# Update salience with manual override marker
sqlite3 "$DB_PATH" <<SQL
BEGIN TRANSACTION;
UPDATE rules
SET salience = $SALIENCE,
    metadata = json_set(COALESCE(metadata, '{}'), '\$.salience_method', 'manual')
WHERE id = '$RULE_ID';
COMMIT;
SQL

# Report success
echo "✓ Updated $RULE_ID: salience = $SALIENCE (manual override)"
echo "  Previous method: $(sqlite3 "$DB_PATH" "SELECT COALESCE(json_extract(metadata, '\$.salience_method'), 'unset') FROM rules WHERE id = '$RULE_ID';" | head -1)"
exit 0
```

---

## Error Scenarios

**Scenario 1: Invalid rule_id format**
```bash
$ /mark-salience adr-001 0.9
ERROR: Invalid rule_id format: adr-001
Expected format: XXX-NNNNN (e.g., ADR-00001)
Exit code: 1
```

**Scenario 2: Salience out of range**
```bash
$ /mark-salience ADR-00001 1.5
ERROR: Salience value out of range: 1.5
Expected: 0.0 <= value <= 1.0
Exit code: 1
```

**Scenario 3: Rule not found**
```bash
$ /mark-salience ADR-99999 0.8
ERROR: Rule ADR-99999 not found in database
Hint: Run 'sqlite3 ... SELECT id, title FROM rules WHERE id LIKE 'ADR-%' ...'
Exit code: 2
```

**Scenario 4: Database error**
```bash
$ /mark-salience ADR-00001 0.9
ERROR: Database not found at: data/rules.db
Exit code: 3
```

**Scenario 5: Schema version mismatch or missing**
```bash
$ /mark-salience ADR-00001 0.9
ERROR: Cannot determine schema version
ERROR: Database may be missing schema_metadata table
HINT: This database may predate Spec 25. Implement Schema Enhancement v1.2.0 first.
Exit code: 4
```

---

## Notes

- **Preserves manual overrides**: Extract.py will NOT overwrite salience values with `salience_method='manual'`
- **Atomic updates**: Transaction ensures salience + metadata updated together
- **Idempotent**: Running same command multiple times has same effect (sets salience to specified value)
- **Audit trail**: metadata.salience_method='manual' tracks that this was HITL override
- **Schema safety**: Verifies schema v1.2.0 before accessing salience column (prevents SQLite errors)
