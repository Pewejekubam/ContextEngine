#!/usr/bin/env bash
# Stage 2: Curate Foundational ADRs
# Constraints: ONB-041, ONB-042, ONB-044, ONB-063, ONB-066

set -e

echo "Stage 2: Curating foundational ADRs..."

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Paths
CANDIDATES_FILE="$PROJECT_ROOT/work/candidates/foundational-adrs.json"
TEMPLATE_FILE="$SCRIPT_DIR/../../templates/onboard-prompt-foundational.txt"
OUTPUT_RAW="$PROJECT_ROOT/work/selections/foundational-raw.txt"
OUTPUT_YAML="$PROJECT_ROOT/work/selections/foundational.yaml"

# Check candidates exist
if [ ! -f "$CANDIDATES_FILE" ]; then
    echo "ERROR: Candidates not found: $CANDIDATES_FILE" >&2
    echo "Run Stage 1 first: build-script-onboard-candidates.py" >&2
    exit 1
fi

# Check template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "ERROR: Template not found: $TEMPLATE_FILE" >&2
    exit 1
fi

# Load candidates
CANDIDATES=$(cat "$CANDIDATES_FILE")
CANDIDATE_COUNT=$(echo "$CANDIDATES" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['metadata']['count'])")

echo "  - Candidates: $CANDIDATE_COUNT"

# ONB-041: Variable substitution ({{VARIABLE}} format for prompts)
TEMPLATE=$(cat "$TEMPLATE_FILE")

# Substitute {{CANDIDATE_COUNT}}
PROMPT="${TEMPLATE//\{\{CANDIDATE_COUNT\}\}/$CANDIDATE_COUNT}"

# Substitute {{CANDIDATES_JSON}} - need to escape for shell
# Extract just the candidates array as JSON string
CANDIDATES_JSON=$(echo "$CANDIDATES" | python3 -c "import sys, json; data=json.load(sys.stdin); print(json.dumps(data['candidates'], indent=2))")

# Use python to do the substitution safely (avoids shell escaping issues)
PROMPT=$(python3 << EOF
import sys
prompt = """$PROMPT"""
candidates_json = """$CANDIDATES_JSON"""
# Replace the variable
result = prompt.replace("{{CANDIDATES_JSON}}", candidates_json)
print(result)
EOF
)

echo "  - Prompt prepared (${#PROMPT} chars)"

# ONB-066: Call Claude with timeout (120s)
echo "  - Calling Claude (this may take ~17 seconds)..."

if timeout 120 echo "$PROMPT" | claude --print > "$OUTPUT_RAW" 2>&1; then
    echo "  - Claude response received"
else
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 124 ]; then
        echo "WARN: Claude timeout on Stage 2" >&2
        echo "selections: []" > "$OUTPUT_YAML"
        exit 0
    else
        echo "ERROR: Claude call failed with exit code $EXIT_CODE" >&2
        echo "selections: []" > "$OUTPUT_YAML"
        exit 0
    fi
fi

# ONB-063: Extract YAML between markers
if ! sed -n '/^---START_SELECTIONS---$/,/^---END_SELECTIONS---$/p' "$OUTPUT_RAW" | \
     grep -v '^---' > "$OUTPUT_YAML"; then
    echo "ERROR: Failed to extract YAML from Claude response" >&2
    echo "selections: []" > "$OUTPUT_YAML"
    exit 0
fi

# Check if output is empty
if [ ! -s "$OUTPUT_YAML" ]; then
    echo "WARN: Claude returned empty response" >&2
    echo "selections: []" > "$OUTPUT_YAML"
    exit 0
fi

# ONB-057: Validate YAML
if ! python3 -c "import yaml, sys; yaml.safe_load(open('$OUTPUT_YAML'))" 2>/dev/null; then
    echo "ERROR: Invalid YAML output from Claude" >&2
    echo "selections: []" > "$OUTPUT_YAML"
    exit 2  # Partial success, degraded output
fi

# Count selections
SELECTION_COUNT=$(python3 -c "import yaml; data=yaml.safe_load(open('$OUTPUT_YAML')); print(len(data.get('selections', [])))")

echo "✓ Stage 2 complete"
echo "  - Foundational ADRs selected: $SELECTION_COUNT"
echo "  - Saved to: $OUTPUT_YAML"
