#!/usr/bin/env bash
# Stage 4: Summarize Project Status (v2.3.0)
# Constraints: ONB-041, ONB-042, ONB-044, ONB-066

set -e

echo "Stage 4: Summarizing project status..."

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Paths
GIT_STATE_FILE="$PROJECT_ROOT/work/candidates/git-state.json"
TEMPLATE_FILE="$SCRIPT_DIR/../../templates/onboard-prompt-summary.txt"
OUTPUT_FILE="$PROJECT_ROOT/work/selections/project-summary.txt"

# Check git state exists
if [ ! -f "$GIT_STATE_FILE" ]; then
    echo "ERROR: Git state not found: $GIT_STATE_FILE" >&2
    exit 1
fi

# Check template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "ERROR: Template not found: $TEMPLATE_FILE" >&2
    exit 1
fi

# Load git state
GIT_LOG=$(python3 -c "import json; data=json.load(open('$GIT_STATE_FILE')); print(data.get('git_log', ''))")
BRANCH=$(python3 -c "import json; data=json.load(open('$GIT_STATE_FILE')); print(data.get('branch', 'unknown'))")

echo "  - Branch: $BRANCH"
echo "  - Git commits available: $(echo "$GIT_LOG" | wc -l)"

# Get chatlog metadata (if available)
CHATLOG_METADATA="No recent chatlogs"

# Try to get chatlog info from database
CONFIG_PATH="$PROJECT_ROOT/context-engine/config/deployment.yaml"
if [ -f "$CONFIG_PATH" ]; then
    DB_PATH=$(grep 'database_path:' "$CONFIG_PATH" | sed 's/.*database_path:\s*["'\'']*//' | sed 's/["'\'']*.*//')
    FULL_DB_PATH="$PROJECT_ROOT/context-engine/$DB_PATH"

    if [ -f "$FULL_DB_PATH" ]; then
        CHATLOG_METADATA=$(python3 << PYEOF
import sqlite3
import json

try:
    conn = sqlite3.connect("$FULL_DB_PATH")
    cursor = conn.execute("""
        SELECT chatlog_id, timestamp, session_focus
        FROM chatlogs
        ORDER BY timestamp DESC
        LIMIT 2
    """)
    rows = cursor.fetchall()

    if rows:
        result = []
        for row in rows:
            chatlog_id, timestamp, focus = row
            result.append(f"- {chatlog_id}: {focus or 'No focus recorded'} ({timestamp})")
        print("\\n".join(result))
    else:
        print("No chatlogs in database")

    conn.close()
except Exception as e:
    print(f"Could not load chatlogs: {e}")
PYEOF
)
    fi
fi

echo "  - Chatlog metadata prepared"

# ONB-041: Variable substitution
# Write variables to temp files to avoid shell escaping issues
TEMP_DIR=$(mktemp -d)
echo "$GIT_LOG" > "$TEMP_DIR/git_log.txt"
echo "$BRANCH" > "$TEMP_DIR/branch.txt"
echo "$CHATLOG_METADATA" > "$TEMP_DIR/chatlog.txt"

# Use python for safe substitution
PROMPT=$(python3 -c "
import sys

with open('$TEMPLATE_FILE', 'r') as f:
    template = f.read()

with open('$TEMP_DIR/git_log.txt', 'r') as f:
    git_log = f.read().strip()

with open('$TEMP_DIR/branch.txt', 'r') as f:
    branch = f.read().strip()

with open('$TEMP_DIR/chatlog.txt', 'r') as f:
    chatlog_metadata = f.read().strip()

# Replace variables
prompt = template.replace('{{CHATLOG_METADATA}}', chatlog_metadata)
prompt = prompt.replace('{{GIT_LOG}}', git_log)
prompt = prompt.replace('{{BRANCH}}', branch)

print(prompt)
")

# Cleanup temp files
rm -rf "$TEMP_DIR"

echo "  - Prompt prepared (${#PROMPT} chars)"

# ONB-066: Call Claude with timeout (lighter workload, ~10s expected)
echo "  - Calling Claude (this may take ~10 seconds)..."

if timeout 120 echo "$PROMPT" | claude --print > "$OUTPUT_FILE" 2>&1; then
    echo "  - Claude response received"
else
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 124 ]; then
        echo "WARN: Claude timeout on Stage 4" >&2
        echo "Project status unavailable due to timeout." > "$OUTPUT_FILE"
        exit 0
    else
        echo "ERROR: Claude call failed with exit code $EXIT_CODE" >&2
        echo "Project status unavailable." > "$OUTPUT_FILE"
        exit 0
    fi
fi

# Check if output is reasonable (150-200 words = ~800-1200 chars)
CHAR_COUNT=$(wc -c < "$OUTPUT_FILE")

if [ "$CHAR_COUNT" -lt 100 ]; then
    echo "WARN: Summary seems too short ($CHAR_COUNT chars)" >&2
fi

if [ "$CHAR_COUNT" -gt 2000 ]; then
    echo "WARN: Summary seems too long ($CHAR_COUNT chars)" >&2
fi

echo "✓ Stage 4 complete"
echo "  - Summary length: $CHAR_COUNT chars"
echo "  - Saved to: $OUTPUT_FILE"
