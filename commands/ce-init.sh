#!/bin/bash
# Context Engine Initialization Tool v2.1.0
# Auto-generated from build/templates/install-template-environment-init-script.sh
# Implements Spec 71 v1.1.0: setup.sh integration + Spec 69 v2.0.0 multi-mode CLI

set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Catch errors in pipes

# ============================================================================
# Environment Setup
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$CE_DIR/.." && pwd)"
VOCAB_FILE="$CE_DIR/config/tag-vocabulary.yaml"
DB_FILE="$CE_DIR/data/rules.db"
VERSION="2.1.0"

# ============================================================================
# INIT-100: Usage Help (no arguments)
# ============================================================================

usage() {
  cat << EOF
Context Engine Initialization Tool v$VERSION

Usage: bash ce-init.sh <command>

Commands:
  --detect-state                    Output current state (FRESH_INSTALL or UPGRADE)
  --discover                        Analyze project structure (JSON output)
  --validate-vocab                  Validate vocabulary file (JSON output)
  <template> --substitute-domains   Inject domain list into template (JSON output)
  --setup                           Initialize deployment (inlined setup logic - Spec 71 v1.1.0)
  --version                         Show version
  --help                            Show this help

Examples:
  bash ce-init.sh --detect-state
  bash ce-init.sh --discover | jq .
  bash ce-init.sh runtime-template-chatlog-capture.md --substitute-domains
  bash ce-init.sh --setup
EOF
  exit 0
}

# ============================================================================
# INIT-001, INIT-002, INIT-003, INIT-101: State Detection
# ============================================================================

detect_state() {
  # INIT-001: Check vocabulary AND database existence
  # INIT-002: FRESH_INSTALL = either artifact missing
  # INIT-003: UPGRADE = both artifacts exist
  # INIT-101: Output single word
  if [ -f "$VOCAB_FILE" ] && [ -f "$DB_FILE" ]; then
    echo "UPGRADE"
  else
    echo "FRESH_INSTALL"
  fi
}

# ============================================================================
# INIT-102, INIT-110 through INIT-116: Discovery Mode
# ============================================================================

discover() {
  # INIT-115: Output JSON object (not array, not plain text)
  # INIT-116: Always exits 0 (discovery never fails)

  # INIT-113: Read first 500 chars of README if exists
  local readme_preview=""
  if [ -f "$PROJECT_ROOT/README.md" ]; then
    readme_preview=$(head -c 500 "$PROJECT_ROOT/README.md" | python3 -c "import sys, json; print(json.dumps(sys.stdin.read()))")
  else
    readme_preview='""'
  fi

  # INIT-112: Count files by extension (depth 3 to avoid noise)
  local py_count
  local md_count
  local yaml_count
  local js_count
  local ts_count
  py_count=$(find "$PROJECT_ROOT" -maxdepth 3 -name "*.py" -type f 2>/dev/null | wc -l | tr -d ' ')
  md_count=$(find "$PROJECT_ROOT" -maxdepth 3 -name "*.md" -type f 2>/dev/null | wc -l | tr -d ' ')
  yaml_count=$(find "$PROJECT_ROOT" -maxdepth 3 \( -name "*.yaml" -o -name "*.yml" \) -type f 2>/dev/null | wc -l | tr -d ' ')
  js_count=$(find "$PROJECT_ROOT" -maxdepth 3 -name "*.js" -type f 2>/dev/null | wc -l | tr -d ' ')
  ts_count=$(find "$PROJECT_ROOT" -maxdepth 3 -name "*.ts" -type f 2>/dev/null | wc -l | tr -d ' ')

  # INIT-111: List directories (exclude hidden except .claude)
  local dirs
  local dir_list
  dir_list=$(ls -d "$PROJECT_ROOT"/*/ 2>/dev/null | xargs -n1 basename 2>/dev/null || true)
  if [ -n "$dir_list" ]; then
    dirs=$(echo "$dir_list" | python3 -c "import sys, json; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))")
  else
    dirs='[]'
  fi

  # INIT-114: Suggest domains based on directory names (basic heuristic)
  local suggested_domains
  local filtered_dirs
  filtered_dirs=$(echo "$dir_list" | grep -vE "^(node_modules|venv|env|build|dist|target|out|bin|obj|__pycache__|\.next|\.nuxt)$" | head -10 || true)
  if [ -n "$filtered_dirs" ]; then
    suggested_domains=$(echo "$filtered_dirs" | python3 -c "import sys, json; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))")
  else
    suggested_domains='[]'
  fi

  # Check for common directories
  local has_tests=false
  local has_docs=false
  local has_src=false
  [ -d "$PROJECT_ROOT/tests" ] || [ -d "$PROJECT_ROOT/test" ] && has_tests=true
  [ -d "$PROJECT_ROOT/docs" ] || [ -d "$PROJECT_ROOT/doc" ] && has_docs=true
  [ -d "$PROJECT_ROOT/src" ] || [ -d "$PROJECT_ROOT/lib" ] && has_src=true

  # Determine primary language
  local primary_language="unknown"
  local max_count=0
  if [ "$py_count" -gt "$max_count" ]; then
    primary_language="python"
    max_count=$py_count
  fi
  if [ "$js_count" -gt "$max_count" ]; then
    primary_language="javascript"
    max_count=$js_count
  fi
  if [ "$ts_count" -gt "$max_count" ]; then
    primary_language="typescript"
    max_count=$ts_count
  fi

  # INIT-110, INIT-115: Output JSON with project analysis
  cat << EOF
{
  "state": "$(detect_state)",
  "project_root": "$PROJECT_ROOT",
  "directories": $dirs,
  "file_types": {
    "python": $py_count,
    "markdown": $md_count,
    "yaml": $yaml_count,
    "javascript": $js_count,
    "typescript": $ts_count
  },
  "readme_exists": $([ -f "$PROJECT_ROOT/README.md" ] && echo true || echo false),
  "readme_preview": $readme_preview,
  "suggested_domains": $suggested_domains,
  "heuristics": {
    "has_tests": $has_tests,
    "has_docs": $has_docs,
    "has_src": $has_src,
    "primary_language": "$primary_language"
  }
}
EOF
}

# ============================================================================
# INIT-103, INIT-120 through INIT-125: Validation Mode
# ============================================================================

validate_vocab() {
  # INIT-125: Exits 0 even if invalid (Claude interprets output)

  local errors=()
  local valid=true

  # INIT-120: Check file exists
  if [ ! -f "$VOCAB_FILE" ]; then
    errors+=("vocabulary file not found: $VOCAB_FILE")
    valid=false
  else
    # INIT-121: Check YAML syntax (requires python3)
    if ! python3 -c "import yaml; yaml.safe_load(open('$VOCAB_FILE'))" 2>/dev/null; then
      errors+=("invalid YAML syntax")
      valid=false
    else
      # INIT-122: Check required keys (tier_1_domains, tier_2_tags, forbidden)
      # Note: Using v3.0.0 schema with nested forbidden structure
      local has_domains
      local has_tags
      local has_forbidden
      has_domains=$(python3 -c "import yaml; d=yaml.safe_load(open('$VOCAB_FILE')); print('tier_1_domains' in d)" 2>/dev/null || echo "False")
      has_tags=$(python3 -c "import yaml; d=yaml.safe_load(open('$VOCAB_FILE')); print('tier_2_tags' in d)" 2>/dev/null || echo "False")
      has_forbidden=$(python3 -c "import yaml; d=yaml.safe_load(open('$VOCAB_FILE')); print('forbidden' in d)" 2>/dev/null || echo "False")

      [ "$has_domains" != "True" ] && errors+=("missing tier_1_domains") && valid=false
      [ "$has_tags" != "True" ] && errors+=("missing tier_2_tags") && valid=false
      [ "$has_forbidden" != "True" ] && errors+=("missing forbidden section") && valid=false

      # INIT-123: Check tier_2_tags has entry for each tier_1_domain
      if [ "$has_domains" = "True" ] && [ "$has_tags" = "True" ]; then
        local missing_tags
        missing_tags=$(python3 << PYEOF
import yaml
with open('$VOCAB_FILE') as f:
    data = yaml.safe_load(f)
domains = set(data.get('tier_1_domains', {}).keys())
tags = set(data.get('tier_2_tags', {}).keys())
missing = domains - tags
if missing:
    print(','.join(missing))
PYEOF
)
        if [ -n "$missing_tags" ]; then
          errors+=("tier_2_tags missing entries for domains: $missing_tags")
          valid=false
        fi
      fi
    fi
  fi

  # INIT-124: Output JSON with valid=true/false and errors array
  local errors_json
  if [ ${#errors[@]} -eq 0 ]; then
    errors_json='[]'
  else
    errors_json=$(printf '%s\n' "${errors[@]}" | python3 -c "import sys, json; print(json.dumps([line.strip() for line in sys.stdin]))")
  fi

  cat << EOF
{
  "valid": $valid,
  "vocab_path": "$VOCAB_FILE",
  "errors": $errors_json
}
EOF
}

# ============================================================================
# INIT-140 through INIT-146: Domain Substitution Mode
# ============================================================================

substitute_domains() {
  local template_file="$1"
  local template_path="$CE_DIR/templates/$template_file"

  # INIT-141: Check template exists
  if [ ! -f "$template_path" ]; then
    echo "{\"success\": false, \"error\": \"Template not found: $template_file\"}"
    exit 1
  fi

  # INIT-140: Check vocabulary exists
  if [ ! -f "$VOCAB_FILE" ]; then
    echo '{"success": false, "error": "Vocabulary file not found"}'
    exit 1
  fi

  # INIT-144: Determine output filename (strip runtime-template- prefix, use .md extension)
  local output_name
  output_name=$(echo "$template_file" | sed 's/^runtime-template-//' | sed 's/\.[^.]*$/.md/')
  local output_path="$CE_DIR/commands/$output_name"

  # INIT-140/142/143: Load vocab, format domains, substitute
  python3 << PYEOF
import yaml
import sys
import json

# Load vocabulary
with open('$VOCAB_FILE') as f:
    vocab = yaml.safe_load(f)

# Load template
with open('$template_path') as f:
    template = f.read()

# INIT-142: Format domain list as markdown
tier_1_domains = vocab.get('tier_1_domains', {})
domain_lines = []
for domain_name, spec in tier_1_domains.items():
    if isinstance(spec, dict):
        description = spec.get('description', 'No description')
    else:
        description = str(spec)
    domain_lines.append(f"- **{domain_name}**: {description}")

domain_list = '\\n'.join(domain_lines)

# INIT-143: Substitute placeholder
if '{domain_list}' not in template:
    print('{"success": false, "error": "Template has no {domain_list} placeholder"}')
    sys.exit(1)

output = template.replace('{domain_list}', domain_list)

# Write output
with open('$output_path', 'w') as f:
    f.write(output)

result = {
    "success": True,
    "template": "$template_file",
    "output": "$output_name",
    "domains_count": len(tier_1_domains)
}
print(json.dumps(result))
PYEOF

  # INIT-145/146: Exit based on Python result
  local exit_code=$?
  exit $exit_code
}

# ============================================================================
# INIT-150 through INIT-193: Setup Mode (inlined from setup.sh, Spec 71 v1.1.0)
# ============================================================================

run_setup() {
  # Colors for output
  local GREEN='\033[0;32m'
  local YELLOW='\033[1;33m'
  local NC='\033[0m' # No Color

  # INIT-150, INIT-151: Auto-detect paths (already set in environment section)
  # SCRIPT_DIR, CE_DIR, PROJECT_ROOT already set

  echo "Context Engine Setup"
  echo "===================="
  echo ""
  echo "Context Engine location: $CE_DIR"
  echo ""
  echo "Your project root is detected as:"
  echo "  $PROJECT_ROOT"
  echo ""
  echo "Using project root: $PROJECT_ROOT"
  echo ""

  # INIT-154 through INIT-185: Initialize config files from .example templates (upgrade-safe)
  echo "Checking configuration files..."

  # INIT-181 through INIT-185: deployment.yaml - validate paths if exists
  local DEPLOYMENT_CONFIG="$CE_DIR/config/deployment.yaml"
  local DEPLOYMENT_EXAMPLE="$CE_DIR/config/deployment.yaml.example"

  # Check for stale config BEFORE preserving
  if [ -f "$DEPLOYMENT_CONFIG" ]; then
    # Extract configured project root from existing config
    local CONFIGURED_ROOT
    CONFIGURED_ROOT=$(python3 -c "
import yaml
try:
    with open('$DEPLOYMENT_CONFIG') as f:
        config = yaml.safe_load(f)
    print(config.get('paths', {}).get('project_root', ''))
except:
    print('')
" 2>/dev/null || echo "")

    # Normalize paths for comparison
    local CONFIGURED_ABS=""
    local DETECTED_ABS
    if [ -n "$CONFIGURED_ROOT" ] && [ -d "$CONFIGURED_ROOT" ]; then
        CONFIGURED_ABS=$(cd "$CONFIGURED_ROOT" 2>/dev/null && pwd || echo "")
    else
        CONFIGURED_ABS=""
    fi
    DETECTED_ABS=$(cd "$PROJECT_ROOT" && pwd)

    # If paths differ, config is stale
    if [ -n "$CONFIGURED_ABS" ] && [ "$CONFIGURED_ABS" != "$DETECTED_ABS" ]; then
        echo -e "${YELLOW}⚠${NC} Stale deployment.yaml detected (from different project)"
        echo "  Configured project: $CONFIGURED_ROOT"
        echo "  Current project:    $PROJECT_ROOT"
        echo "  Regenerating with current paths..."
        rm "$DEPLOYMENT_CONFIG"
    elif [ -z "$CONFIGURED_ABS" ]; then
        echo -e "${YELLOW}⚠${NC} Invalid deployment.yaml detected (bad paths)"
        echo "  Regenerating with current paths..."
        rm "$DEPLOYMENT_CONFIG"
    else
        echo -e "${GREEN}✓${NC} Existing deployment.yaml valid (paths match current location)"
    fi
  fi

  # Create deployment.yaml if missing
  if [ ! -f "$DEPLOYMENT_CONFIG" ]; then
    if [ -f "$DEPLOYMENT_EXAMPLE" ]; then
        cp "$DEPLOYMENT_EXAMPLE" "$DEPLOYMENT_CONFIG"
        echo -e "${GREEN}✓${NC} Created deployment.yaml from example"

        # Update paths in the new config
        python3 << EOF
import yaml
from pathlib import Path

config_path = Path('$DEPLOYMENT_CONFIG')
with open(config_path) as f:
    config = yaml.safe_load(f)

config['paths']['project_root'] = '$PROJECT_ROOT'
config['paths']['context_engine_home'] = '$CE_DIR'
config['paths']['commands_dir'] = '$PROJECT_ROOT/.claude/commands'

with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False, sort_keys=False)
EOF
    else
        echo -e "${YELLOW}⚠${NC} Warning: deployment.yaml.example not found"
    fi
  fi

  # tag-vocabulary.yaml
  local VOCAB_CONFIG="$CE_DIR/config/tag-vocabulary.yaml"
  local VOCAB_EXAMPLE="$CE_DIR/config/tag-vocabulary.yaml.example"

  if [ -f "$VOCAB_CONFIG" ]; then
    echo -e "${GREEN}✓${NC} Preserving existing tag-vocabulary.yaml (your custom tags kept)"
    if [ -f "$VOCAB_EXAMPLE" ]; then
        echo "  Note: New example vocabulary available at tag-vocabulary.yaml.example"
        echo "  Review for new tier-2 tags and vocabulary mappings"
    fi
  else
    if [ -f "$VOCAB_EXAMPLE" ]; then
        cp "$VOCAB_EXAMPLE" "$VOCAB_CONFIG"
        echo -e "${GREEN}✓${NC} Created tag-vocabulary.yaml from example"
    else
        echo -e "${YELLOW}⚠${NC} Warning: tag-vocabulary.yaml.example not found"
    fi
  fi

  echo ""

  # INIT-161 through INIT-166: Create database if it doesn't exist
  local DB_PATH="$CE_DIR/data/rules.db"
  local SCHEMA_PATH="$CE_DIR/schema/schema.sql"

  if [ ! -f "$DB_PATH" ]; then
    if [ -f "$SCHEMA_PATH" ]; then
        echo "Creating database from schema..."
        sqlite3 "$DB_PATH" < "$SCHEMA_PATH"
        echo -e "${GREEN}✓${NC} Database created: data/rules.db"
    else
        echo "⚠ Warning: schema.sql not found, skipping database creation"
    fi
  else
    echo -e "${GREEN}✓${NC} Database already exists (preserving data)"

    # Check schema version compatibility
    echo "Checking schema compatibility..."

    # Get database version
    local DB_VERSION
    DB_VERSION=$(sqlite3 "$DB_PATH" "SELECT value FROM schema_metadata WHERE key='schema_version'" 2>/dev/null || echo "unknown")

    # Get package version from schema.sql header comment
    local PACKAGE_VERSION
    if [ -f "$SCHEMA_PATH" ]; then
        PACKAGE_VERSION=$(grep -oP -- '-- Context Engine Database Schema v\K[0-9.]+' "$SCHEMA_PATH" 2>/dev/null || echo "unknown")
    else
        PACKAGE_VERSION="unknown"
    fi

    if [ "$DB_VERSION" = "unknown" ]; then
        echo -e "${YELLOW}⚠${NC} Warning: Cannot determine database schema version"
        echo "  This database may be from an older Context Engine version"
        echo "  without schema_metadata table. Proceeding with caution..."
    elif [ "$DB_VERSION" != "$PACKAGE_VERSION" ]; then
        echo ""
        echo -e "${YELLOW}⚠ WARNING: Schema version mismatch!${NC}"
        echo "  Database version:  $DB_VERSION"
        echo "  Package version:   $PACKAGE_VERSION"
        echo ""
        echo "This package may NOT be compatible with your existing database."
        echo ""
        echo -e "${YELLOW}⚠${NC} WARNING: Schema version mismatch (continuing anyway)"
        echo "  See docs/UPGRADE.md for migration guidance if needed"
        echo ""
    else
        echo -e "${GREEN}✓${NC} Schema version compatible: $DB_VERSION"
    fi
  fi

  # INIT-167 through INIT-169: Create required directories
  echo "Creating directories..."

  mkdir -p "$CE_DIR/data/chatlogs"
  mkdir -p "$PROJECT_ROOT/.claude/commands"

  echo -e "${GREEN}✓${NC} All directories created"

  # INIT-170, INIT-171: Create symlinks for ce-* slash commands (not copies)
  echo ""
  echo "Installing slash commands..."

  local symlinks_created=0
  local symlinks_skipped=0
  for cmd_file in "$CE_DIR/commands/ce-"*.md; do
    if [ -f "$cmd_file" ]; then
      local cmd_name
      cmd_name=$(basename "$cmd_file")
      local target="$PROJECT_ROOT/.claude/commands/$cmd_name"
      local source="../../.context-engine/commands/$cmd_name"

      # Idempotent: Skip if symlink exists and points to correct target
      if [ -L "$target" ]; then
        local current_target
        current_target=$(readlink "$target")
        if [ "$current_target" = "$source" ]; then
          symlinks_skipped=$((symlinks_skipped + 1))
          continue  # Already correct
        fi
        rm "$target"  # Wrong target, recreate
      elif [ -f "$target" ]; then
        # Regular file exists, replace with symlink
        rm "$target"
      fi

      ln -s "$source" "$target"
      symlinks_created=$((symlinks_created + 1))
    fi
  done

  # INIT-172 through INIT-173: Output completion message
  echo -e "${GREEN}✓${NC} Slash commands installed"
  echo ""
  echo "========================================================================"
  echo -e "${GREEN}✓ Setup complete!${NC}"
  echo "========================================================================"
  echo ""
  echo "Next steps:"
  echo "  1. Review configuration: config/deployment.yaml"
  echo "  2. Customize domains: config/tag-vocabulary.yaml"
  echo "  3. Capture knowledge: /ce-capture slash command in Claude Code"
  echo "  4. Extract to database: python3 scripts/extract.py"
  echo ""
  echo "Documentation: docs/GETTING_STARTED.md"
  echo ""

  # INIT-130: Output JSON summary for orchestration
  echo "{\"success\": true, \"message\": \"Setup completed successfully\", \"symlinks_created\": $symlinks_created, \"symlinks_skipped\": $symlinks_skipped}"
  exit 0
}

# ============================================================================
# INIT-105: Version
# ============================================================================

show_version() {
  echo "$VERSION"
}

# ============================================================================
# INIT-100, INIT-106: Main CLI Dispatch
# ============================================================================

# INIT-100: No arguments = usage help
if [ $# -eq 0 ]; then
  usage
fi

# INIT-106: Unknown switches print error and usage help, exit 1
case "${1:-}" in
  --detect-state)
    detect_state
    ;;
  --discover)
    discover
    ;;
  --validate-vocab)
    validate_vocab
    ;;
  --setup)
    run_setup
    ;;
  --version)
    show_version
    ;;
  --help|-h)
    usage
    ;;
  *)
    # Check for two-argument form: <template> --substitute-domains
    if [ $# -ge 2 ] && [ "${2:-}" = "--substitute-domains" ]; then
      substitute_domains "$1"
    else
      echo "Error: Unknown command '$1'" >&2
      echo "Run 'bash ce-init.sh --help' for usage" >&2
      exit 1
    fi
    ;;
esac
