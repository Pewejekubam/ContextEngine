#!/usr/bin/env python3
"""
Stage 1: Generate Candidates for Onboard Pipeline (v2.1.0)
Extracts candidate sets from database for Claude curation with composite scoring

Constraints: ONB-037 to ONB-040, ONB-048 to ONB-052, ONB-056, ONB-060 to ONB-062
             CS-001 through CS-070 (Composite Scoring - Spec 31)
"""

import sqlite3
import json
import sys
import subprocess
import math
from pathlib import Path
from datetime import datetime, timezone

# ONB-038: Work Directory Initialization
SCRIPT_DIR = Path(__file__).resolve().parent
# Runtime: .context-engine/scripts/onboard/ -> .context-engine (parents[1])
# Build: build/scripts/onboard-pipeline/ -> project root (parents[2])
# Detect which environment we're in
if SCRIPT_DIR.name == 'onboard':
    # Runtime deployment: .context-engine/scripts/onboard/
    CONTEXT_ENGINE_HOME = SCRIPT_DIR.parents[1]
    PROJECT_ROOT = CONTEXT_ENGINE_HOME.parent
else:
    # Build environment: build/scripts/onboard-pipeline/
    PROJECT_ROOT = SCRIPT_DIR.parents[2]
    CONTEXT_ENGINE_HOME = PROJECT_ROOT / "context-engine"

WORK_DIR = PROJECT_ROOT / "work"
CANDIDATES_DIR = WORK_DIR / "candidates"
SELECTIONS_DIR = WORK_DIR / "selections"

# Create work directory structure
CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
SELECTIONS_DIR.mkdir(parents=True, exist_ok=True)

print("Stage 1: Generating candidates...")

# Load config (from context-engine home, not PROJECT_ROOT/context-engine)
config_path = CONTEXT_ENGINE_HOME / "config" / "deployment.yaml"
if not config_path.exists():
    print(f"ERROR: Config not found: {config_path}", file=sys.stderr)
    sys.exit(1)

# Parse deployment.yaml
import re
config = {}
with open(config_path, 'r') as f:
    content = f.read()
    # Extract structure paths
    db_match = re.search(r'database_path:\s*["\']?([^"\'\n]+)', content)
    chatlogs_match = re.search(r'chatlogs_dir:\s*["\']?([^"\'\n]+)', content)

    if db_match:
        config['database_path'] = db_match.group(1)
    if chatlogs_match:
        config['chatlogs_dir'] = chatlogs_match.group(1)

# Resolve paths
db_path = CONTEXT_ENGINE_HOME / config.get('database_path', 'data/rules.db')
vocab_path = CONTEXT_ENGINE_HOME / 'config' / 'tag-vocabulary.yaml'

print(f"  - Database: {db_path}")
print(f"  - Vocabulary: {vocab_path}")

# Check database exists
if not db_path.exists():
    print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
    sys.exit(1)

# Connect to database
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

# ONB-051, ONB-052: Load tier 1 domains from vocabulary
tier_1_domains = []
if vocab_path.exists():
    try:
        import yaml
        with open(vocab_path, 'r') as f:
            vocab = yaml.safe_load(f)
            # Spec 23 v1.1.0 schema: tier_1_domains is dict {domain_name: {description, aliases}}
            tier_1_domains_dict = vocab.get('tier_1_domains', {})
            tier_1_domains = list(tier_1_domains_dict.keys())
        print(f"  - Tier 1 domains: {len(tier_1_domains)}")
    except Exception as e:
        print(f"WARNING: Vocabulary unavailable: {e}", file=sys.stderr)
        tier_1_domains = []
else:
    print(f"WARNING: Vocabulary file not found: {vocab_path}", file=sys.stderr)

# ONB-048, ONB-049, ONB-050: Git state extraction (non-critical)
git_log = ""
branch = "unknown"

try:
    # ONB-048: Git log query
    result = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "log", "-20", "--format=%h %s", "--no-merges"],
        capture_output=True,
        text=True,
        timeout=5
    )
    if result.returncode == 0:
        git_log = result.stdout.strip()
        print(f"  - Git commits: {len(git_log.splitlines())}")
    else:
        print("WARNING: Git log unavailable (not a repository or no commits)", file=sys.stderr)
except Exception as e:
    print(f"WARNING: Git log failed: {e}", file=sys.stderr)

try:
    # ONB-049: Branch detection
    result = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "branch", "--show-current"],
        capture_output=True,
        text=True,
        timeout=5
    )
    if result.returncode == 0:
        branch = result.stdout.strip()
        if branch == "":
            branch = "detached HEAD"
        print(f"  - Branch: {branch}")
    else:
        print("WARNING: Git branch detection failed", file=sys.stderr)
except Exception as e:
    print(f"WARNING: Git branch detection failed: {e}", file=sys.stderr)


# CS-030c, CS-070b: Logging functions for composite scoring
def log_warning(rule_id, missing_field, default_value):
    """Log missing data warning to composite_score_warnings.log"""
    log_path = CONTEXT_ENGINE_HOME / "data" / "composite_score_warnings.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    with open(log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{missing_field}\t{default_value}\n")


def log_error(rule_id, error_type, details, fallback_value):
    """Log calculation error to composite_score_errors.log"""
    log_path = CONTEXT_ENGINE_HOME / "data" / "composite_score_errors.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    with open(log_path, 'a') as f:
        f.write(f"{timestamp}\t{rule_id}\t{error_type}\t{details}\t{fallback_value}\n")


def calculate_composite_score(rule, now):
    """
    CS-001: Calculate composite score for onboarding candidate ranking.

    Formula: 0.4×salience + 0.3×confidence + 0.2×recency + 0.1×scope

    Args:
        rule: Dictionary with rule data (must have 'rule_id' key)
        now: Current datetime (timezone-aware) for recency calculation

    Returns:
        float: Composite score (0.0-1.0), rounded to 4 decimal places
    """
    rule_id = rule.get('rule_id', 'UNKNOWN')

    try:
        # CS-001a: Salience factor
        salience = rule.get('salience')
        if salience is None:
            salience = 0.7  # Default per CS-030
            log_warning(rule_id, 'salience', '0.7')

        # CS-001b: Confidence factor
        confidence = rule.get('confidence', 0.0)

        # CS-020: Recency factor (dual-phase exponential decay)
        created_at_str = rule.get('created_at', '')
        try:
            # CS-020d: Parse ISO 8601 with Z suffix
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            days_old = (now - created_at).days

            # CS-020: Dual-phase exponential decay
            recency = math.exp(-0.03 * days_old) + 0.25 * math.exp(-0.003 * days_old)
            recency = min(recency, 1.0)  # CS-001c: Clamp to 0.0-1.0
        except (ValueError, AttributeError, TypeError) as e:
            log_error(rule_id, 'TIMESTAMP_PARSE_ERROR', f'Invalid timestamp: {created_at_str}', 'recency=0.5')
            recency = 0.5  # Neutral default on error

        # CS-021: Scope bonus factor
        metadata = rule.get('metadata', {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata) if metadata else {}
            except:
                metadata = {}

        scope = metadata.get('reusability_scope', 'project_wide')
        scope_bonus = 1.0 if scope == 'project_wide' else 0.5  # CS-021

        # CS-001: Weighted composite score
        composite = (
            0.4 * salience +
            0.3 * confidence +
            0.2 * recency +
            0.1 * scope_bonus
        )

        return round(composite, 4)  # 4 decimal places sufficient

    except Exception as e:
        # CS-070: Fallback to confidence on calculation error
        fallback = rule.get('confidence', 0.0)
        log_error(rule_id, 'CALCULATION_ERROR', str(e), f'confidence={fallback}')
        return fallback


def format_candidate(row):
    """
    ONB-037: Candidate JSON Structure
    CS-040b: Enhanced with salience, knowledge_type, composite_score
    Convert database row to candidate JSON with full context
    """
    # Parse tags (stored as JSON array in database)
    tags = []
    if row['tags']:
        try:
            tags = json.loads(row['tags'])
        except:
            tags = []

    # Parse metadata
    metadata = {}
    if row['metadata']:
        try:
            metadata = json.loads(row['metadata'])
        except:
            metadata = {}

    # CS-030a: Extract knowledge_type with uniform default
    knowledge_type = metadata.get('knowledge_type', {
        'reference': 0.2,
        'procedure': 0.2,
        'decision': 0.2,
        'incident': 0.2,
        'pattern': 0.2
    })

    return {
        "rule_id": row['id'],
        "rule_type": row['type'],
        "title": row['title'] or "",
        "decision": row['description'] or "",  # For ADRs, description = decision
        "rationale": metadata.get('rationale', ""),
        "context": metadata.get('context', ""),
        "confidence": row['confidence'] if row['confidence'] is not None else 0.0,
        "salience": row['salience'] if 'salience' in row.keys() and row['salience'] is not None else None,  # CS-040b: Add salience (may be None, handled in composite_score)
        "knowledge_type": knowledge_type,  # CS-040b: Add knowledge_type
        "reusability_scope": metadata.get('reusability_scope', ""),
        "domain": row['domain'] or "",
        "tags": tags,
        "created_at": row['created_at'] or "",
        "chatlog_source": metadata.get('chatlog_title', ""),
        "metadata": metadata  # Store metadata for composite_score calculation
    }


def truncate_candidates(candidates, limit, query_type):
    """
    ONB-040: Candidate Set Truncation
    CS-040c: Sort by composite_score DESC and truncate if > limit
    """
    if len(candidates) > limit:
        original_count = len(candidates)
        # CS-040c: Sort by composite_score DESC
        candidates.sort(key=lambda c: c.get('composite_score', 0.0), reverse=True)
        # Truncate
        candidates = candidates[:limit]
        print(f"WARNING: Truncated {query_type} from {original_count} to {limit} (sorted by composite_score DESC)", file=sys.stderr)
    return candidates


# ONB-056: Query 1 - Foundational ADRs
# CS-040: Calculate composite_score for ranking
print("\nQuerying foundational ADRs...")
cursor = conn.execute("""
    SELECT id, type, title, description, domain, confidence, salience,
           COALESCE(tags, '[]') as tags, metadata, created_at
    FROM rules
    WHERE type = 'ADR'
      AND lifecycle = 'active'
      AND confidence >= 0.9
      AND json_extract(metadata, '$.reusability_scope') = 'project_wide'
      AND tags_state != 'needs_tags'
    ORDER BY confidence DESC
    LIMIT 150
""")

foundational_candidates = [format_candidate(row) for row in cursor.fetchall()]

# CS-040: Calculate composite_score for each candidate
now = datetime.now(timezone.utc)
for candidate in foundational_candidates:
    candidate['composite_score'] = calculate_composite_score(candidate, now)
    # Remove internal metadata field (only needed for composite_score calculation)
    candidate.pop('metadata', None)

foundational_candidates = truncate_candidates(foundational_candidates, 150, "foundational_adrs")

# ONB-039: Empty database graceful degradation
if len(foundational_candidates) == 0:
    print("WARNING: No foundational_adrs candidates found", file=sys.stderr)

print(f"  - Foundational ADRs: {len(foundational_candidates)}")

# Save foundational candidates
foundational_output = {
    "candidates": foundational_candidates,
    "metadata": {
        "count": len(foundational_candidates),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "query_type": "foundational_adrs",
        "source_database": str(db_path.resolve())
    }
}

with open(CANDIDATES_DIR / "foundational-adrs.json", 'w') as f:
    json.dump(foundational_output, f, indent=2)


# ONB-062: Query 2 - Recent ADRs (via chatlog foreign key)
print("\nQuerying recent ADRs...")

# Get last 2 chatlog IDs
cursor = conn.execute("""
    SELECT chatlog_id
    FROM chatlogs
    ORDER BY timestamp DESC
    LIMIT 2
""")
recent_chatlog_ids = [row['chatlog_id'] for row in cursor.fetchall()]

if len(recent_chatlog_ids) == 0:
    print("WARNING: No chatlogs found, recent queries will be empty", file=sys.stderr)
    recent_adrs = []
    recent_patterns = []
else:
    placeholders = ','.join(['?' for _ in recent_chatlog_ids])

    # Get ADRs from recent chatlogs
    cursor = conn.execute(f"""
        SELECT id, type, title, description, domain, confidence, salience,
               COALESCE(tags, '[]') as tags, metadata, created_at
        FROM rules
        WHERE type = 'ADR'
          AND lifecycle = 'active'
          AND chatlog_id IN ({placeholders})
          AND tags_state != 'needs_tags'
        ORDER BY confidence DESC
        LIMIT 150
    """, recent_chatlog_ids)

    recent_adrs = [format_candidate(row) for row in cursor.fetchall()]

    # CS-040: Calculate composite_score for each candidate
    for candidate in recent_adrs:
        candidate['composite_score'] = calculate_composite_score(candidate, now)
        # Remove internal metadata field (only needed for composite_score calculation)
        candidate.pop('metadata', None)

    recent_adrs = truncate_candidates(recent_adrs, 150, "recent_adrs")

    if len(recent_adrs) == 0:
        print("WARNING: No recent_adrs candidates found", file=sys.stderr)

    print(f"  - Recent ADRs: {len(recent_adrs)}")

    # Query 3 - Recent Patterns (CON/INV from recent chatlogs)
    print("\nQuerying recent patterns...")
    cursor = conn.execute(f"""
        SELECT id, type, title, description, domain, confidence, salience,
               COALESCE(tags, '[]') as tags, metadata, created_at
        FROM rules
        WHERE type IN ('CON', 'INV')
          AND lifecycle = 'active'
          AND chatlog_id IN ({placeholders})
          AND tags_state != 'needs_tags'
        ORDER BY confidence DESC, type ASC
        LIMIT 150
    """, recent_chatlog_ids)

    recent_patterns = [format_candidate(row) for row in cursor.fetchall()]

    # CS-040: Calculate composite_score for each candidate
    for candidate in recent_patterns:
        candidate['composite_score'] = calculate_composite_score(candidate, now)
        # Remove internal metadata field (only needed for composite_score calculation)
        candidate.pop('metadata', None)

    recent_patterns = truncate_candidates(recent_patterns, 150, "recent_patterns")

    if len(recent_patterns) == 0:
        print("WARNING: No recent_patterns candidates found", file=sys.stderr)

    print(f"  - Recent Patterns: {len(recent_patterns)}")

# Save recent ADRs
recent_adrs_output = {
    "candidates": recent_adrs,
    "metadata": {
        "count": len(recent_adrs),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "query_type": "recent_adrs",
        "source_database": str(db_path.resolve())
    }
}

with open(CANDIDATES_DIR / "recent-adrs.json", 'w') as f:
    json.dump(recent_adrs_output, f, indent=2)

# Save recent patterns
recent_patterns_output = {
    "candidates": recent_patterns,
    "metadata": {
        "count": len(recent_patterns),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "query_type": "recent_patterns",
        "source_database": str(db_path.resolve())
    }
}

with open(CANDIDATES_DIR / "recent-patterns.json", 'w') as f:
    json.dump(recent_patterns_output, f, indent=2)

# Query chatlog metadata for freshness indicators
chatlog_count = 0
latest_chatlog_date = None
try:
    cursor = conn.execute("SELECT COUNT(*) as count FROM chatlogs")
    row = cursor.fetchone()
    if row:
        chatlog_count = row[0]

    # Get latest chatlog timestamp
    cursor = conn.execute("SELECT timestamp FROM chatlogs ORDER BY timestamp DESC LIMIT 1")
    row = cursor.fetchone()
    if row and row[0]:
        latest_chatlog_date = row[0]
except Exception as e:
    print(f"WARNING: Could not query chatlog metadata: {e}", file=sys.stderr)

# Save git state and vocabulary for Stage 4 and Stage 5
state_output = {
    "git_log": git_log,
    "branch": branch,
    "tier_1_domains": tier_1_domains,
    "chatlog_count": chatlog_count,
    "latest_chatlog_date": latest_chatlog_date,
    "generated_at": datetime.now(timezone.utc).isoformat()
}

with open(CANDIDATES_DIR / "git-state.json", 'w') as f:
    json.dump(state_output, f, indent=2)

conn.close()

print(f"\n✓ Stage 1 complete")
print(f"  - Candidates saved to: {CANDIDATES_DIR}")
print(f"  - Total candidate sets: 3 (foundational, recent ADRs, recent patterns)")
