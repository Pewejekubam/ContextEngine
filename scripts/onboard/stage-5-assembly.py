#!/usr/bin/env python3
"""
Stage 5: Assemble Final Onboard Artifact
Merges selections into onboard-root.yaml using domain-indexed template

Constraints: ONB-042, ONB-043, ONB-045 to ONB-047, ONB-053 to ONB-055, ONB-064,
             ONB-070, ONB-070a, ONB-070b, ONB-071, ONB-071a, ONB-072, ONB-073, ONB-073a,
             RREL-007, RREL-007a, RREL-007b, RREL-007c
"""

import sqlite3
import json
import sys
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

print("Stage 5: Assembling onboard-root.yaml...")


def calculate_composite_score(rule, now):
    """
    Calculate composite score for onboarding candidate ranking.

    Implements Spec 31 CS-001: 0.4×salience + 0.3×confidence + 0.2×recency + 0.1×scope

    Args:
        rule: Dict with keys: salience, confidence, created_at, metadata
        now: Current datetime (timezone-aware)

    Returns:
        Float composite score (0.0-1.0 range)
    """
    import math

    # Salience factor (CS-001a) - default 0.7 per CS-030
    salience = rule.get('salience', 0.7)

    # Confidence factor (CS-001b)
    confidence = rule.get('confidence', 0.0)

    # Recency factor (CS-020) - exponential decay
    created_at_str = rule.get('created_at', '')
    try:
        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        days_old = (now - created_at).days
        # Dual exponential: fast decay (30-day half-life) + slow decay (200-day half-life)
        recency = math.exp(-0.03 * days_old) + 0.25 * math.exp(-0.003 * days_old)
        recency = min(recency, 1.0)  # Clamp to 0.0-1.0
    except (ValueError, AttributeError):
        recency = 0.5  # Neutral default for parse errors

    # Scope bonus factor (CS-021)
    metadata = rule.get('metadata', {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata) if metadata else {}
        except:
            metadata = {}
    scope = metadata.get('reusability_scope', 'project_wide')
    scope_bonus = 1.0 if scope == 'project_wide' else 0.5

    # Composite score (CS-001)
    composite = (
        0.4 * salience +
        0.3 * confidence +
        0.2 * recency +
        0.1 * scope_bonus
    )

    return round(composite, 4)  # 4 decimal places sufficient


# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
# Runtime: .context-engine/scripts/onboard/ -> .context-engine (parents[1])
# Build: build/scripts/onboard-pipeline/ -> project root (parents[2])
if SCRIPT_DIR.name == 'onboard':
    # Runtime deployment
    CONTEXT_ENGINE_HOME = SCRIPT_DIR.parents[1]
    PROJECT_ROOT = CONTEXT_ENGINE_HOME.parent
else:
    # Build environment
    PROJECT_ROOT = SCRIPT_DIR.parents[2]
    CONTEXT_ENGINE_HOME = PROJECT_ROOT / "context-engine"

WORK_DIR = PROJECT_ROOT / "work"
SELECTIONS_DIR = WORK_DIR / "selections"
CANDIDATES_DIR = WORK_DIR / "candidates"

# Load selections
foundational_file = SELECTIONS_DIR / "foundational.yaml"
recent_file = SELECTIONS_DIR / "recent.yaml"
summary_file = SELECTIONS_DIR / "project-summary.txt"
git_state_file = CANDIDATES_DIR / "git-state.json"
template_file = CONTEXT_ENGINE_HOME / "templates" / "runtime-template-onboard-root.md"

# Check files exist
for f in [foundational_file, recent_file, summary_file, git_state_file, template_file]:
    if not f.exists():
        print(f"ERROR: Required file not found: {f}", file=sys.stderr)
        sys.exit(1)

# Load YAML selections
import yaml

with open(foundational_file, 'r') as f:
    foundational_data = yaml.safe_load(f)

with open(recent_file, 'r') as f:
    recent_data = yaml.safe_load(f)

with open(summary_file, 'r') as f:
    project_summary = f.read().strip()

with open(git_state_file, 'r') as f:
    git_state = json.load(f)

# ONB-071, ONB-071a: Load tag vocabulary to discover tier_1_domains
vocab_file = CONTEXT_ENGINE_HOME / "config" / "tag-vocabulary.yaml"
if not vocab_file.exists():
    print(f"ERROR: Tag vocabulary not found: {vocab_file}", file=sys.stderr)
    sys.exit(1)

with open(vocab_file, 'r') as f:
    vocabulary = yaml.safe_load(f)

# ONB-071a: tier_1_domains must be a dict (schema contract)
tier_1_domains_dict = vocabulary.get('tier_1_domains', {})
if not isinstance(tier_1_domains_dict, dict):
    print(f"ERROR: tier_1_domains must be a dict, got {type(tier_1_domains_dict)}", file=sys.stderr)
    sys.exit(1)

tier_1_domains = list(tier_1_domains_dict.keys())
print(f"  - Tier 1 domains loaded: {len(tier_1_domains)}")

# Extract rule IDs from selections
foundational_selections = foundational_data.get('selections', [])
recent_adr_selections = recent_data.get('recent_adrs', [])
recent_pattern_selections = recent_data.get('recent_patterns', [])

print(f"  - Foundational ADRs selected: {len(foundational_selections)}")
print(f"  - Recent ADRs selected: {len(recent_adr_selections)}")
print(f"  - Recent patterns selected: {len(recent_pattern_selections)}")

# Collect all rule IDs
all_rule_ids = set()

for sel in foundational_selections:
    if 'rule_id' in sel:
        all_rule_ids.add(sel['rule_id'])

for sel in recent_adr_selections:
    if 'rule_id' in sel:
        all_rule_ids.add(sel['rule_id'])

for sel in recent_pattern_selections:
    if 'rule_id' in sel:
        all_rule_ids.add(sel['rule_id'])

print(f"  - Total unique rules to fetch: {len(all_rule_ids)}")

# Load config to get database path
config_path = CONTEXT_ENGINE_HOME / "config" / "deployment.yaml"
import re
config = {}
with open(config_path, 'r') as f:
    content = f.read()
    db_match = re.search(r'database_path:\s*["\']?([^"\'\n]+)', content)
    if db_match:
        config['database_path'] = db_match.group(1)
db_path = CONTEXT_ENGINE_HOME / config.get('database_path', 'data/rules.db')

if not db_path.exists():
    print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
    sys.exit(1)

# Connect to database and fetch full rule data
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

# ONB-073a: Query all selected rules with chatlog timestamp
# ONB-070a: composite_score computed per Spec 31, not stored in DB
placeholders = ','.join(['?' for _ in all_rule_ids])
if len(all_rule_ids) > 0:
    cursor = conn.execute(f"""
        SELECT r.id, r.type, r.title, r.description, r.domain, r.confidence,
               r.salience, COALESCE(r.tags, '[]') as tags,
               r.metadata, r.created_at, r.chatlog_id,
               c.timestamp as chatlog_timestamp
        FROM rules r
        LEFT JOIN chatlogs c ON r.chatlog_id = c.chatlog_id
        WHERE r.id IN ({placeholders})
    """, list(all_rule_ids))

    # Get current time for composite_score calculation (Spec 31 CS-020)
    now = datetime.now(timezone.utc)

    rules_data = {}
    for row in cursor.fetchall():
        # Parse tags and metadata
        tags = []
        if row['tags']:
            try:
                tags = json.loads(row['tags'])
            except:
                tags = []

        metadata = {}
        if row['metadata']:
            try:
                metadata = json.loads(row['metadata'])
            except:
                metadata = {}

        # Build rule dict for composite_score calculation
        rule_dict = {
            'id': row['id'],
            'type': row['type'],
            'title': row['title'] or "",
            'description': row['description'] or "",
            'rationale': metadata.get('rationale', ""),
            'context': metadata.get('context', ""),
            'confidence': row['confidence'] if row['confidence'] is not None else 0.0,
            'salience': row['salience'] if row['salience'] is not None else 0.5,
            'domain': row['domain'] or "",
            'tags': tags,
            'created_at': row['created_at'] or "",
            'chatlog_id': row['chatlog_id'] or "",
            'chatlog_timestamp': row['chatlog_timestamp'] or "",
            'metadata': metadata
        }

        # ONB-070a: Compute composite_score per Spec 31 (not stored in database)
        rule_dict['composite_score'] = calculate_composite_score(rule_dict, now)

        rules_data[row['id']] = rule_dict
else:
    rules_data = {}

# Note: Keep connection open for RREL-007 relationship queries during rendering
# Connection will be closed after domain sections are rendered

print(f"  - Rules fetched from database: {len(rules_data)}")


def get_rule_relationships(conn, rule_id, all_rules_dict):
    """
    RREL-007: Query forward relationships where this rule is the target.

    Searches all rules for relationships pointing to the given rule_id.
    Groups relationships by type: implements, extends, conflicts_with, related_to.

    Args:
        conn: Database connection
        rule_id: Target rule ID to find relationships for
        all_rules_dict: Dict of all curated rules {rule_id: rule_data}

    Returns:
        Dict with keys for each relationship type, values are lists of {id, title} dicts
    """
    relationships = {
        'implements': [],
        'extends': [],
        'conflicts_with': [],
        'related_to': []
    }

    # Search through all curated rules for relationships
    for other_rule_id, other_rule in all_rules_dict.items():
        if other_rule_id == rule_id:
            continue  # Skip self

        metadata = other_rule.get('metadata', {})
        rule_relationships = metadata.get('relationships', [])

        # Check each relationship
        for rel in rule_relationships:
            if rel.get('target') == rule_id:
                rel_type = rel.get('type', '')
                if rel_type in relationships:
                    relationships[rel_type].append({
                        'id': other_rule_id,
                        'title': other_rule.get('title', 'Untitled')
                    })

    return relationships


def get_implementation_refs(metadata):
    """
    RREL-007b: Extract implementation_refs from rules.metadata.

    Categorizes refs by type: implements (code), validates (tests), documents (docs).

    Args:
        metadata: Rule metadata dict

    Returns:
        Dict with keys 'code', 'tests', 'docs', values are lists of ref dicts
    """
    implementation_refs = metadata.get('implementation_refs', [])

    categorized = {
        'code': [],
        'tests': [],
        'docs': []
    }

    for ref in implementation_refs:
        ref_type = ref.get('type', '')
        if ref_type == 'implements':
            categorized['code'].append(ref)
        elif ref_type == 'validates':
            categorized['tests'].append(ref)
        elif ref_type == 'documents':
            categorized['docs'].append(ref)

    return categorized


def render_rule_entry(rule, conn, all_rules_dict):
    """
    ONB-073: Render single rule with full metadata
    RREL-007, RREL-007a, RREL-007b, RREL-007c: Add optional relationship cross-references and code pointers
    """
    tags_str = ", ".join(rule.get('tags', []))
    description = rule.get('description', '')
    if len(description) > 500:
        description = description[:500] + "..."

    rationale = rule.get('rationale', '')
    if rationale and len(rationale) > 500:
        rationale = rationale[:500] + "..."

    entry = f"""### {rule['id']}: {rule['title']}

**Type**: {rule['type']} | **Confidence**: {rule['confidence']:.2f} | **Salience**: {rule.get('salience', 0.5):.2f}
**Tags**: {tags_str}
**Domain**: {rule.get('domain', 'general')}

**Decision**: {description}
"""

    if rationale:
        entry += f"\n**Rationale**: {rationale}\n"

    entry += f"""
**Metadata**:
- Chatlog: {rule['chatlog_id']} ({rule.get('chatlog_timestamp', 'unknown')})
- Created: {rule['created_at']}
- Composite Score: {rule.get('composite_score', 0):.3f}
"""

    # RREL-007, RREL-007a: Optional relationship cross-references
    relationships = get_rule_relationships(conn, rule['id'], all_rules_dict)

    if relationships['implements']:
        entry += "\n**Implemented by**:\n"
        for impl in relationships['implements']:
            entry += f"- {impl['id']}: {impl['title']}\n"

    if relationships['extends']:
        entry += "\n**Extended by**:\n"
        for ext in relationships['extends']:
            entry += f"- {ext['id']}: {ext['title']}\n"

    if relationships['conflicts_with']:
        entry += "\n**Conflicts with**:\n"
        for conf in relationships['conflicts_with']:
            entry += f"- {conf['id']}: {conf['title']}\n"

    if relationships['related_to']:
        entry += "\n**Related decisions**:\n"
        for rel in relationships['related_to']:
            entry += f"- {rel['id']}: {rel['title']}\n"

    # RREL-007b, RREL-007c: Optional implementation reference sections
    metadata = rule.get('metadata', {})
    impl_refs = get_implementation_refs(metadata)

    if impl_refs['code']:
        entry += "\n**Implementation: Code**:\n"
        for ref in impl_refs['code']:
            file_path = ref.get('file', '')
            line_range = f" (lines {ref['lines']})" if 'lines' in ref else ""
            role_desc = f": {ref['role']}" if ref.get('role') else ""
            entry += f"- {file_path}{line_range}{role_desc}\n"

    if impl_refs['tests']:
        entry += "\n**Implementation: Tests**:\n"
        for ref in impl_refs['tests']:
            file_path = ref.get('file', '')
            line_range = f" (lines {ref['lines']})" if 'lines' in ref else ""
            role_desc = f": {ref['role']}" if ref.get('role') else ""
            entry += f"- {file_path}{line_range}{role_desc}\n"

    if impl_refs['docs']:
        entry += "\n**Implementation: Docs**:\n"
        for ref in impl_refs['docs']:
            file_path = ref.get('file', '')
            line_range = f" (lines {ref['lines']})" if 'lines' in ref else ""
            role_desc = f": {ref['role']}" if ref.get('role') else ""
            entry += f"- {file_path}{line_range}{role_desc}\n"

    return entry


def get_primary_domain(rule, tier_1_domains, domain_avg_scores):
    """
    ONB-070b: Assign rule to primary domain based on rule['domain'] field

    Algorithm:
    1. Get rule's domain from domain field (not tags)
    2. If domain in tier_1_domains: return that domain
    3. If domain not in tier_1_domains: return None (unassigned)
    """
    rule_domain = rule.get('domain', '')

    if rule_domain in tier_1_domains:
        return rule_domain

    return None


def render_domain_sections(domain_groups, vocabulary, conn, all_rules_dict):
    """
    ONB-072, ONB-073: Render domain-indexed sections with navigation anchors
    ONB-071: Skip empty domains (no placeholder sections)
    RREL-007: Pass conn and all_rules_dict for relationship queries
    """
    sections = []

    for domain_key, rules in domain_groups.items():
        if not rules:  # ONB-071: Skip empty domains
            continue

        # ONB-072: Generate section heading with navigation anchor
        domain_data = vocabulary['tier_1_domains'][domain_key]
        display_name = domain_data.get('name', domain_key.replace('_', ' ').title())

        section = f"## {display_name} {{#{domain_key}}}\n\n"

        # Add domain description if available
        if 'description' in domain_data:
            section += f"*{domain_data['description']}*\n\n"

        # ONB-073, RREL-007: Render each rule in domain with relationships and code pointers
        for rule in rules:
            section += render_rule_entry(rule, conn, all_rules_dict)
            section += "\n---\n\n"

        sections.append(section)

    return "\n".join(sections)


# ONB-070: Collect all curated rules (no duplicates from selections)
curated_rules = []
seen_rule_ids = set()

for sel in foundational_selections:
    rule_id = sel.get('rule_id')
    if rule_id in rules_data and rule_id not in seen_rule_ids:
        curated_rules.append(rules_data[rule_id])
        seen_rule_ids.add(rule_id)

for sel in recent_adr_selections:
    rule_id = sel.get('rule_id')
    if rule_id in rules_data and rule_id not in seen_rule_ids:
        curated_rules.append(rules_data[rule_id])
        seen_rule_ids.add(rule_id)

for sel in recent_pattern_selections:
    rule_id = sel.get('rule_id')
    if rule_id in rules_data and rule_id not in seen_rule_ids:
        curated_rules.append(rules_data[rule_id])
        seen_rule_ids.add(rule_id)

print(f"  - Total curated rules (deduplicated): {len(curated_rules)}")

# ONB-070b: Pre-calculate domain average composite scores (using domain field)
domain_avg_scores = {}
for domain_key in tier_1_domains:
    domain_rules_temp = [r for r in curated_rules if r.get('domain', '') == domain_key]
    if domain_rules_temp:
        avg_score = sum(r.get('composite_score', 0) for r in domain_rules_temp) / len(domain_rules_temp)
        domain_avg_scores[domain_key] = avg_score
        print(f"  - Domain '{domain_key}': {len(domain_rules_temp)} rules, avg score {avg_score:.3f}")

# ONB-070: Assign each rule to primary domain (no duplicates across domains)
domain_groups = {d: [] for d in tier_1_domains}
assigned_rules = set()

for rule in curated_rules:
    primary = get_primary_domain(rule, tier_1_domains, domain_avg_scores)
    if primary and rule['id'] not in assigned_rules:
        domain_groups[primary].append(rule)
        assigned_rules.add(rule['id'])

# ONB-070a: Sort within each domain by composite_score DESC
for domain_key in domain_groups:
    domain_groups[domain_key].sort(
        key=lambda x: x.get('composite_score', 0),
        reverse=True
    )

print(f"  - Rules assigned to domains: {len(assigned_rules)}")
print(f"  - Rules not assigned (no tier_1_domain tags): {len(curated_rules) - len(assigned_rules)}")

# Load output template
with open(template_file, 'r') as f:
    template = f.read()

# Prepare variables (snake_case per coding standards)
version = "v2.1.0"  # Onboard pipeline version
pipeline_version = "2.1.0"
timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Freshness indicators: git commit hash at generation time
import subprocess
try:
    last_commit_hash = subprocess.check_output(
        ['git', 'rev-parse', '--short', 'HEAD'],
        cwd=PROJECT_ROOT,
        stderr=subprocess.DEVNULL,
        text=True
    ).strip()
except (subprocess.CalledProcessError, FileNotFoundError):
    last_commit_hash = "unknown"

# Freshness indicators: chatlog metadata from git_state
git_state_file = CANDIDATES_DIR / "git-state.json"
chatlog_count = 0
latest_chatlog_date = "unknown"
if git_state_file.exists():
    try:
        with open(git_state_file, 'r') as f:
            git_state = json.load(f)
            chatlog_count = git_state.get('chatlog_count', 0)
            # Extract date part from ISO 8601 timestamp
            chatlog_timestamp = git_state.get('latest_chatlog_date')
            if chatlog_timestamp:
                latest_chatlog_date = chatlog_timestamp.split('T')[0]  # Extract date only
    except (json.JSONDecodeError, KeyError, ValueError):
        pass  # Use defaults

# ONB-072, ONB-073, RREL-007: Render domain-indexed sections with relationships
domain_sections = render_domain_sections(domain_groups, vocabulary, conn, rules_data)

# RREL-007: Close database connection after rendering (kept open for relationship queries)
conn.close()

# RREL-009a: Prepare Getting Started section static content
essential_commands = """- `/ce-capture` - Capture session knowledge to chatlog
- `/ce-extract` - Extract rules from recent chatlogs
- `/ce-tags-optimize` - Tag rules with domain and metadata
- `/ce-onboard-generate` - Regenerate this onboarding file"""

key_directories = """- `data/` - Rules database and chatlogs
- `config/` - Deployment config and tag vocabulary
- `scripts/` - Operational scripts (extract, classify, optimize)
- `docs/` - Architectural documentation"""

rule_type_legend = """- **ADR** (Architectural Decision Record) - Major design decisions
- **CON** (Constraint) - Active constraints and requirements
- **INV** (Invariant) - System invariants that must never be violated"""

# ONB-042: Variable substitution for ${variable} format
template = template.replace("${version}", version)
template = template.replace("${timestamp}", timestamp)
template = template.replace("${pipeline_version}", pipeline_version)
template = template.replace("${db_path}", str(db_path.resolve()))
template = template.replace("${project_summary}", project_summary)
template = template.replace("${domain_sections}", domain_sections)
template = template.replace("${last_commit_hash}", last_commit_hash)
template = template.replace("${chatlog_count}", str(chatlog_count))
template = template.replace("${latest_chatlog_date}", latest_chatlog_date)
template = template.replace("${essential_commands}", essential_commands)
template = template.replace("${key_directories}", key_directories)
template = template.replace("${rule_type_legend}", rule_type_legend)

final_content = template

print(f"  - Final artifact size: {len(final_content)} chars")

# ONB-047: Output path validation
OUTPUT_PATH = PROJECT_ROOT / "onboard-root.yaml"

if not PROJECT_ROOT.exists():
    print(f"ERROR: PROJECT_ROOT does not exist: {PROJECT_ROOT}", file=sys.stderr)
    sys.exit(1)

if not PROJECT_ROOT.is_dir():
    print(f"ERROR: PROJECT_ROOT is not a directory: {PROJECT_ROOT}", file=sys.stderr)
    sys.exit(1)

# ONB-045, ONB-046: Atomic write with error handling
try:
    # ONB-045: Create temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as tmp:
        tmp.write(final_content)
        tmp_path = tmp.name

    # Copy to final location (works across filesystems)
    shutil.copy2(tmp_path, str(OUTPUT_PATH))

    # Clean up temp file
    Path(tmp_path).unlink(missing_ok=True)

    print(f"\n✓ Stage 5 complete")
    print(f"  - Generated: {OUTPUT_PATH}")
    print(f"  - Size: {OUTPUT_PATH.stat().st_size} bytes")

except (OSError, IOError) as e:
    # ONB-046: Filesystem error handling
    if 'tmp_path' in locals():
        Path(tmp_path).unlink(missing_ok=True)
    print(f"ERROR: Failed to write {OUTPUT_PATH}: {e}", file=sys.stderr)
    sys.exit(1)
