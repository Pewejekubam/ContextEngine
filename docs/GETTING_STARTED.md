# Getting Started with Context Engine

The Context Engine is a **portable, drop-in tool** for capturing and managing
engineering decisions, constraints, and architectural knowledge across projects.

## What You Can Do

- **Capture** session knowledge as structured chatlogs (YAML)
- **Extract** rules into SQLite database for queryability
- **Optimize** tags using Claude reasoning with human oversight
- **Generate** agent onboarding context from accumulated knowledge

---

## Installation

1. Extract the distribution:
   ```bash
   tar -xf context-engine-runtime-v3.4.1.tar
   cd .context-engine
   ```

2. **First-time initialization** (discovers project-specific domains).

   Ask Claude:
   ```
   Please process commands/ce-init.md
   ```

   The initialization prompt will:
   - Detect installation state (fresh install, upgrade, or migration)
   - Discover project domains automatically (fresh install)
   - Generate project-specific vocabulary
   - Configure paths and initialize the database
   - Preserve existing configs (upgrades)

   See `docs/INITIALIZATION.md` for detailed guide.

3. Verify installation:
   ```bash
   make help
   ```

---

## Context Hygiene

Context Engine captures **session knowledge** - architectural decisions, constraints, and patterns from your work.

### When to `/clear`

Run `/clear` to reset conversation context after:

1. **Initialization/Setup** - After running commands/ce-init.md
2. **CE Maintenance** - After fixing bugs in Context Engine itself
3. **Context Switches** - Before switching from infrastructure to feature work

### Why This Matters

If you `/ce-capture` after CE initialization, you'll pollute your project database with rules like:
- "Setup.sh supports non-interactive mode" (CE infrastructure, not your project)
- "Vocabulary regeneration uses template substitution" (CE internals, not your domain)

These aren't your project's knowledge - they're Context Engine's implementation details.

### Professional Discipline

Think of `/clear` like git hygiene:
- Don't mix refactoring commits with feature commits
- Don't capture tool maintenance in project knowledge

**Rule of thumb**: If the session was about **fixing/configuring the tool**, `/clear` before `/ce-capture`.

---

## First Capture

Capture session knowledge using the `/ce-capture` slash command in Claude Code.

This creates a structured chatlog in `data/chatlogs/` with:
- Decisions (ADR), Constraints (CON), Invariants (INV)
- Session context and provenance metadata
- Quality validation and warnings

**Example:**
```bash
/ce-capture
```

Follow the prompts to record what you learned in this session.

---

## Command Options

Context Engine provides two ways to run workflows:

| Method | Best For | Notes |
|--------|----------|-------|
| **Slash commands** (`/ce-*`) | Convenience from within Claude Code | Some invoke `claude --print` (adds latency) |
| **Make targets** | CLI, bulk operations, interactive workflows | Run from `.context-engine` directory |

**Performance note:** Slash commands that invoke Claude (like `/ce-tags-optimize-all` and `/ce-onboard-generate`) have latency overhead. For bulk operations with many rules, prefer the `make` targets.

---

## Core Workflows

### Extract Chatlogs to Database

Process all unprocessed chatlogs and insert rules into SQLite.

**Slash command** (from Claude Code):
```
/ce-extract
```

**Make target** (from terminal):
```bash
cd .context-engine
make chatlogs-extract
```

### Debug Chatlogs

Debug chatlogs with verbose JSON validation output (for troubleshooting schema issues):

```bash
cd .context-engine
make chatlogs-debug
```

Or validate a specific chatlog:

```bash
make chatlogs-validate FILE=data/chatlogs/<your-chatlog>.yaml
```

### Optimize Tags (Interactive)

Refine tags for rules using vocabulary-aware Claude reasoning with human-in-the-loop approval.

**Make target only** (interactive workflow):
```bash
cd .context-engine
make tags-optimize
```

This opens an interactive session where you approve/reject tag suggestions one by one. Not available as a slash command due to the interactive nature.

### Optimize Tags (Batch)

Auto-approve tags meeting quality thresholds (confidence ≥0.70, coherence ≥0.30).

**Slash command** (from Claude Code):
```
/ce-tags-optimize-all
```

**Make target** (from terminal):
```bash
cd .context-engine
make tags-optimize-auto
```

**Note:** For large rule sets, the `make` target is faster. The slash command invokes `claude --print` for each rule, which adds latency.

### View Statistics

Check database status and tag usage:

```bash
cd .context-engine
make database-status  # Show rule counts by type and tags_state
make tags-stats       # Display tag frequency histogram
```

### Generate Onboarding Context

Create agent onboarding YAML with recent work and curated rules.

**Slash command** (from Claude Code):
```
/ce-onboard-generate
```

**Make target** (from terminal):
```bash
cd .context-engine
make onboard-generate
```

Output: `onboard-root.yaml` in your project root.

**Note:** This runs a 5-stage pipeline with multiple Claude invocations. The `make` target provides better progress visibility for large knowledge bases.

### Full Pipeline

Run the complete ETL workflow:

```bash
cd .context-engine
make ci-pipeline
```

This runs: extract → optimize-tags → validation

---

## Configuration

### deployment.yaml

Located at `config/deployment.yaml`. Key settings:

```yaml
paths:
  project_root: /path/to/your/project
  context_engine_home: /path/to/your/project/.context-engine
  commands_dir: /path/to/your/project/.claude/commands

structure:
  chatlogs_dir: data/chatlogs
  database_path: data/rules.db

behavior:
  rule_id_format: "{TYPE}-{NNNNN}"
```

Updated automatically during initialization.

### tag-vocabulary.yaml

Located at `config/tag-vocabulary.yaml`. Customize:

- `tier_1_domains`: Top-level categories for your project
- `tier_2_tags`: Specific tags within each domain
- `vocabulary_mappings`: Common term → canonical tag mappings
- `forbidden`: Stopwords and overly broad terms

---

## Next Steps

1. **Review discovered domains**: Check `config/tag-vocabulary.yaml` (auto-generated during initialization)
2. **Refine vocabulary** (optional): Add tier-2 tags for each domain as needed
3. **Capture a session**: Use `/ce-capture` after meaningful work
4. **Run the pipeline**: `make ci-pipeline` to process chatlogs
5. **Query your knowledge**: Use `sqlite3 data/rules.db` for custom queries

## Upgrading

See `docs/UPGRADE.md` for version migration instructions.

## Troubleshooting

**Database not found:**
```bash
make database-status
# If missing, re-run initialization
bash commands/ce-init.sh --setup
```

**Chatlogs not processing:**
```bash
# Validate chatlog format
make chatlogs-validate FILE=data/chatlogs/<your-chatlog>.yaml
```

**Schema version mismatch:**
- See UPGRADE.md for migration steps
- Backup database before upgrading
