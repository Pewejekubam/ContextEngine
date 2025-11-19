# Context Engine Initialization Guide

This guide explains how Context Engine initialization works and how to use it effectively.

---

## Overview

Context Engine initialization is a **one-shot, automated process** that configures the system for your project by:

1. **Detecting installation state**: Fresh install, upgrade, or migration required
2. **Discovering project domains**: Analyzing your codebase to identify appropriate domain vocabulary
3. **Generating configuration**: Creating project-specific tag vocabulary
4. **Running setup**: Configuring paths and initializing the database

**Key principle**: No human-in-the-loop during execution - the process runs autonomously based on evidence gathered from your project.

---

## When to Run Initialization

### Fresh Installation

**Trigger**: You've extracted Context Engine for the first time in a project.

**What happens**:
- Scans your project structure, README, and code
- Discovers 6-12 project-specific domains automatically
- Generates `config/tag-vocabulary.yaml` with discovered domains
- Creates `config/deployment.yaml` with absolute paths
- Initializes SQLite database
- Reports discovered domains with evidence

**Command**:
```bash
Ask Claude: "Please process .context-engine-init.md"
```

### Upgrade (Compatible Versions)

**Trigger**: You've extracted a new Context Engine version over an existing installation with compatible schema.

**What happens**:
- Detects existing configuration files
- Preserves your custom vocabulary and settings
- Updates scripts and runtime files
- Re-runs setup if needed
- Reports what was preserved and updated

**Command**:
```bash
Ask Claude: "Please process .context-engine-init.md"
```

### Migration Required

**Trigger**: Database schema version doesn't match package version.

**What happens**:
- Detects version mismatch
- Reports current and target versions
- Provides migration guidance from `docs/UPGRADE.md`
- Exits without running setup (you must migrate first)

**Command**:
```bash
Ask Claude: "Please process .context-engine-init.md"
```

After completing migration steps, re-run initialization.

---

## How Discovery Works

The discovery process examines your project to identify appropriate domains using multiple heuristics:

### High-Signal Artifacts (Examined First)

1. **Configuration manifests**: package.json, pyproject.toml, Cargo.toml, go.mod, pom.xml
2. **Documentation**: README.md, ARCHITECTURE.md, CONTRIBUTING.md
3. **Directory structure**: Top-level subdirectories under src/, lib/, or project root
4. **Build configs**: Dockerfile, docker-compose.yml, .github/workflows/, Makefile

### Pattern Recognition

The system looks for:

- **System names**: Proper nouns in README (Architecture, Components sections)
- **Directory patterns**: Top-level directories with 5+ files (excluding tests)
- **File naming**: Module prefixes like `qbd_*.py`, namespace patterns like `src/qbd/`
- **Architectural keywords**: Repeated patterns like "service", "repository", "controller"
- **Import frequency**: Module names imported 5+ times across codebase
- **Test organization**: Test directories that mirror domain structure

### Validation Rules

Each candidate domain must:

- Have **2+ sources of evidence** (e.g., README mention + dedicated directory)
- Use lowercase alphanumeric with hyphens/underscores only
- Be 2-20 characters long
- Avoid forbidden patterns (utils, common, shared, test, build, dist, node_modules)

### Conflict Resolution

The system automatically handles:

- **Synonyms**: Merges "db" and "database", "auth" and "authentication"
- **Singular/plural**: Uses singular form ("user" not "users")
- **Overlap**: Detects when one domain is a subset of another

### Quality Assessment

**HIGH confidence** (auto-commits):
- 6-12 domains discovered
- Strong evidence (2+ sources per domain)
- No forbidden names
- No unresolved conflicts

**MEDIUM confidence** (reports for review):
- 4-5 or 13-15 domains
- Some weak evidence
- Potential conflicts

**LOW confidence** (reports failure):
- <4 or >15 domains
- Mostly generic names
- Insufficient evidence

---

## Example: QBD-to-GnuCash Project

Here's what initialization discovers for a typical ETL project:

**Discovered Domains (6):**

1. **qbd**: QuickBooks Desktop source data structures
   - Evidence: Directory `src/qbd/` (12 files), README mentions "QBD IIF format"

2. **gc**: GnuCash target format and validation
   - Evidence: Directory `src/gc/` (8 files), README mentions "GnuCash XML"

3. **pipeline**: ETL transformation orchestration
   - Evidence: Files `pipeline.py`, `orchestrator.py`, README "ETL pipeline"

4. **transformation**: Field mapping and conversion logic
   - Evidence: Directory `src/transformation/` (6 files), `mapper.py`

5. **validation**: Data quality and constraint checking
   - Evidence: Directory `src/validation/` (4 files), `constraints.py`

6. **testing**: Test infrastructure
   - Evidence: Directory `tests/` (15 files), test fixtures

**Confidence**: HIGH (all domains have 2+ evidence sources)

**Ambiguities Resolved**:
- "parser" vs "qbd": Merged "parser" into "qbd" (parser.py in src/qbd/ directory)

---

## Customizing Discovered Domains

After initialization, you can refine the vocabulary:

### Reviewing Discovered Domains

Check `config/tag-vocabulary.yaml`:

```yaml
tier_1_domains:
  - qbd
  - gc
  - pipeline
  - transformation
  - validation
  - testing

tier_2_tags:
  qbd: []  # Add specific tags as you use them
  gc: []
  # ...
```

### Adding Tier-2 Tags

As you capture rules, add domain-specific tags:

```yaml
tier_2_tags:
  qbd:
    - iif_format
    - quickbooks_entities
    - data_extraction
  gc:
    - xml_generation
    - account_mapping
    - gnucash_validation
```

### Adjusting Domains

If discovery produced incorrect results:

1. Edit `config/tag-vocabulary.yaml` directly
2. Add, remove, or rename domains as needed
3. Regenerate capture command:
   ```bash
   # This will be supported in future versions
   # For now, manually edit .claude/commands/ce-capture.md
   ```

---

## Troubleshooting

### Low Confidence Results

**Symptom**: Initialization reports LOW confidence, <4 or >15 domains.

**Solutions**:
1. Review discovered domains in the output
2. Manually create `config/tag-vocabulary.yaml` with appropriate domains
3. Use `config/tag-vocabulary.yaml.example` as a template
4. Re-run initialization

### Generic Domain Names

**Symptom**: Discovered domains are too generic (utils, common, lib).

**Why**: Your project may not have clear architectural boundaries, or uses generic naming.

**Solution**: Manually specify domains based on your mental model:
- What are the major functional areas?
- How would you explain the system to a new developer?
- What are the distinct responsibilities?

### Too Many Domains

**Symptom**: Initialization discovers 15+ domains.

**Why**: Your project has many small modules or deep directory structure.

**Solution**:
1. Review the prioritization in the output
2. Manually reduce to 8-12 top-level domains
3. Consider grouping related modules under umbrella domains

### Stub Domains Remain

**Symptom**: After initialization, domains are still `__DOMAIN_1__`, etc.

**Why**: Initialization wasn't run, or setup.sh was run directly.

**Solution**:
```bash
Ask Claude: "Please process .context-engine-init.md"
```

---

## Advanced: Recovery Scenarios

### Corrupted Configuration

If you manually corrupted configs (copied .example files before running init):

```bash
# Delete corrupted configs
rm config/deployment.yaml config/tag-vocabulary.yaml .claude/commands/ce-capture.md

# Re-run initialization
# Ask Claude: "Please process .context-engine-init.md"
```

### Failed Fresh Install

If initialization failed mid-flight:

1. Re-run initialization - the system has **self-healing**:
   - Detects partial state as upgrade scenario
   - Re-runs setup.sh to complete installation
   - No explicit rollback needed

---

## Success Metrics

Context Engine initialization targets:

- **<10% user intervention rate**: 90%+ of discovered domains should be relevant without manual editing
- **6-12 domains**: Optimal range for tag vocabulary (4-15 acceptable)
- **HIGH confidence**: Strong evidence for all domains

If you find yourself frequently editing discovered domains, please report the project characteristics so we can improve the heuristics.

---

## Technical Details

For technical implementation details, see:
- `.context-engine-init.md`: The orchestration prompt (technical specification)
- `build/modules/install-command-environment-init.yaml`: Constraint definitions (INIT-001 through INIT-050)
- `build/docs/03_deployment_initialization_architecture-plan-v1.1.0.md`: Architecture design document

---

## Feedback

If initialization produces poor results for your project, please report:
- Project type (web app, CLI tool, library, etc.)
- Directory structure characteristics
- What domains were discovered vs. what you expected
- Confidence level reported

This helps improve the discovery heuristics for future releases.
