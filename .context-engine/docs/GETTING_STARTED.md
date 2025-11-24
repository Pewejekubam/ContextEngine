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

1. Download and extract the distribution:

   **Download latest release automatically:**
   ```bash
   curl -s https://api.github.com/repos/Pewejekubam/ContextEngine/releases/latest | grep "browser_download_url.*tar" | cut -d '"' -f 4 | wget -qi -
   ```

   **Or download a specific version** (v3.3.0):

   Using curl:
   ```bash
   curl -LO https://github.com/Pewejekubam/ContextEngine/releases/download/v3.3.0/context-engine-runtime-v3.3.0-20251124-130820Z.tar
   ```

   Using wget:
   ```bash
   wget https://github.com/Pewejekubam/ContextEngine/releases/download/v3.3.0/context-engine-runtime-v3.3.0-20251124-130820Z.tar
   ```

   Extract and enter directory:
   ```bash
   tar -xf context-engine-runtime-*.tar
   cd .context-engine
   ```

2. **First-time initialization** (discovers project-specific domains):

   Ask Claude:
   ```
   Please process .context-engine-init.md
   ```

   The initialization prompt will:
   - Detect installation state (fresh install, upgrade, or migration)
   - Discover project domains automatically (fresh install)
   - Generate project-specific vocabulary
   - Run setup.sh to configure paths and database
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

1. **Initialization/Setup** - After running .context-engine-init.md
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

## Core Workflows

### Extract Chatlogs to Database

Process all unprocessed chatlogs and insert rules into SQLite:

```bash
make chatlogs-extract
```

### Debug Chatlogs

Debug chatlogs with verbose JSON validation output (for troubleshooting schema issues):

```bash
make chatlogs-debug
```

Or validate a specific chatlog:

```bash
make chatlogs-validate FILE=data/chatlogs/<your-chatlog>.yaml
```

### Optimize Tags

Refine tags for rules using vocabulary-aware Claude reasoning:

```bash
make tags-optimize
```

This opens a human-in-the-loop workflow where you approve/reject tag suggestions.

### View Statistics

Check database status and tag usage:

```bash
make database-status  # Show rule counts by type and tags_state
make tags-stats       # Display tag frequency histogram
```

### Generate Onboarding Context

Create agent onboarding YAML with recent work and curated rules:

```bash
make onboard-generate
```

Output: `onboard-root.yaml` in your project root.

### Full Pipeline

Run the complete ETL workflow:

```bash
make ci-pipeline
```

This runs: extract → optimize-tags → validation

---

## Configuration

### deployment.yaml

Located at `config/deployment.yaml`. Key settings:

```yaml
external:
  project_root: $HOME/my-project
  context_engine_home: $HOME/my-project/.context-engine

internal:
  database_path: data/rules.db
  chatlogs_dir: data/chatlogs
```

Updated automatically by `setup.sh`.

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
# If missing, setup.sh should have created it
./setup.sh
```

**Chatlogs not processing:**
```bash
# Validate chatlog format
make chatlogs-validate FILE=data/chatlogs/<your-chatlog>.yaml
```

**Schema version mismatch:**
- See UPGRADE.md for migration steps
- Backup database before upgrading
