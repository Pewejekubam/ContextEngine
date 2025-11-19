#!/usr/bin/env bash
# Stage 3: Curate Recent Activity (ADRs + Patterns)
# Constraints: ONB-041, ONB-042, ONB-044, ONB-057, ONB-063, ONB-066

set -e

echo "Stage 3: Curating recent activity..."

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Paths
ADRS_FILE="$PROJECT_ROOT/work/candidates/recent-adrs.json"
PATTERNS_FILE="$PROJECT_ROOT/work/candidates/recent-patterns.json"
TEMPLATE_FILE="$SCRIPT_DIR/../../templates/onboard-prompt-recent.txt"
OUTPUT_RAW="$PROJECT_ROOT/work/selections/recent-raw.txt"
OUTPUT_YAML="$PROJECT_ROOT/work/selections/recent.yaml"

# Check candidates exist
if [ ! -f "$ADRS_FILE" ]; then
    echo "ERROR: Recent ADRs not found: $ADRS_FILE" >&2
    exit 1
fi

if [ ! -f "$PATTERNS_FILE" ]; then
    echo "ERROR: Recent patterns not found: $PATTERNS_FILE" >&2
    exit 1
fi

# Check template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "ERROR: Template not found: $TEMPLATE_FILE" >&2
    exit 1
fi

# Load candidates
ADRS=$(cat "$ADRS_FILE")
PATTERNS=$(cat "$PATTERNS_FILE")

ADR_COUNT=$(echo "$ADRS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['metadata']['count'])")
PATTERN_COUNT=$(echo "$PATTERNS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['metadata']['count'])")

echo "  - Recent ADRs: $ADR_COUNT"
echo "  - Recent Patterns: $PATTERN_COUNT"

# ONB-041: Variable substitution
TEMPLATE=$(cat "$TEMPLATE_FILE")

# Prepare JSON strings
RECENT_ADRS_JSON=$(echo "$ADRS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(json.dumps(data['candidates'], indent=2))")
RECENT_PATTERNS_JSON=$(echo "$PATTERNS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(json.dumps(data['candidates'], indent=2))")

# Use python to do substitution safely
PROMPT=$(python3 << EOF
import sys
prompt = """$TEMPLATE"""

# Replace variables
prompt = prompt.replace("{{ADR_COUNT}}", "$ADR_COUNT")
prompt = prompt.replace("{{PATTERN_COUNT}}", "$PATTERN_COUNT")
prompt = prompt.replace("{{RECENT_ADRS_JSON}}", """$RECENT_ADRS_JSON""")
prompt = prompt.replace("{{RECENT_PATTERNS_JSON}}", """$RECENT_PATTERNS_JSON""")

print(prompt)
EOF
)

echo "  - Prompt prepared (${#PROMPT} chars)"

# ONB-066: Call Claude with timeout
echo "  - Calling Claude (this may take ~17 seconds)..."

if timeout 120 echo "$PROMPT" | claude --print > "$OUTPUT_RAW" 2>&1; then
    echo "  - Claude response received"
else
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 124 ]; then
        echo "WARN: Claude timeout on Stage 3" >&2
        echo -e "recent_adrs: []\nrecent_patterns: []" > "$OUTPUT_YAML"
        exit 0
    else
        echo "ERROR: Claude call failed with exit code $EXIT_CODE" >&2
        echo -e "recent_adrs: []\nrecent_patterns: []" > "$OUTPUT_YAML"
        exit 0
    fi
fi

# ONB-063: Extract YAML between markers
if ! sed -n '/^---START_SELECTIONS---$/,/^---END_SELECTIONS---$/p' "$OUTPUT_RAW" | \
     grep -v '^---' > "$OUTPUT_YAML"; then
    echo "ERROR: Failed to extract YAML from Claude response" >&2
    echo -e "recent_adrs: []\nrecent_patterns: []" > "$OUTPUT_YAML"
    exit 0
fi

# Check if output is empty
if [ ! -s "$OUTPUT_YAML" ]; then
    echo "WARN: Claude returned empty response" >&2
    echo -e "recent_adrs: []\nrecent_patterns: []" > "$OUTPUT_YAML"
    exit 0
fi

# ONB-057: Validate YAML (must have both recent_adrs and recent_patterns keys)
if ! python3 -c "
import yaml
import sys

try:
    with open('$OUTPUT_YAML', 'r') as f:
        data = yaml.safe_load(f)

    # Verify required keys exist
    if 'recent_adrs' not in data:
        print('ERROR: Missing recent_adrs key in YAML', file=sys.stderr)
        sys.exit(1)

    if 'recent_patterns' not in data:
        print('ERROR: Missing recent_patterns key in YAML', file=sys.stderr)
        sys.exit(1)

    # Verify they're lists
    if not isinstance(data['recent_adrs'], list):
        print('ERROR: recent_adrs must be a list', file=sys.stderr)
        sys.exit(1)

    if not isinstance(data['recent_patterns'], list):
        print('ERROR: recent_patterns must be a list', file=sys.stderr)
        sys.exit(1)

    sys.exit(0)

except Exception as e:
    print(f'ERROR: Invalid YAML: {e}', file=sys.stderr)
    sys.exit(1)
"; then
    echo "ERROR: YAML validation failed" >&2
    echo -e "recent_adrs: []\nrecent_patterns: []" > "$OUTPUT_YAML"
    exit 2  # Partial success, degraded output
fi

# Count selections
ADR_SELECTION_COUNT=$(python3 -c "import yaml; data=yaml.safe_load(open('$OUTPUT_YAML')); print(len(data.get('recent_adrs', [])))")
PATTERN_SELECTION_COUNT=$(python3 -c "import yaml; data=yaml.safe_load(open('$OUTPUT_YAML')); print(len(data.get('recent_patterns', [])))")

echo "✓ Stage 3 complete"
echo "  - Recent ADRs selected: $ADR_SELECTION_COUNT"
echo "  - Recent patterns selected: $PATTERN_SELECTION_COUNT"
echo "  - Saved to: $OUTPUT_YAML"
