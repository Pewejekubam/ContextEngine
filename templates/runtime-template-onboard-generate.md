# Generate Agent Onboarding Context

> Context Engine: Generate onboard-root.yaml for new agent sessions

<!-- TEMPLATE_METADATA
Template: runtime-template-onboard-generate
Version: v1.0.0
Updated: 2025-11-30
Variables: None (self-contained)

Changelog:
- v1.0.0 (2025-11-30): Initial template for /ce-onboard-generate slash command
END_TEMPLATE_METADATA -->

## Overview

Generate the `onboard-root.yaml` artifact containing curated project context for new agent sessions.

**What this does:**
- Extracts candidate rules from database (foundational + recent)
- Uses Claude to curate most important ADRs and patterns
- Generates project summary from git history and chatlogs
- Assembles domain-indexed onboard-root.yaml
- Places output in project root (external to .context-engine)

**When to use:**
- Before starting a new Claude Code session
- After significant knowledge capture (`/ce-capture` + `/ce-extract`)
- To refresh onboarding context with recent architectural decisions

---

## Run Generation

```bash
# Run from .context-engine directory
cd .context-engine
make onboard-generate
```

The command executes a 5-stage pipeline:

1. **Stage 1**: Generate candidates (Python) - extracts rules from database
2. **Stage 2**: Curate foundational (Claude) - selects 10-15 core ADRs
3. **Stage 3**: Curate recent (Claude) - selects recent ADRs and patterns
4. **Stage 4**: Summarize status (Claude) - generates project summary
5. **Stage 5**: Assemble artifact (Python) - produces final YAML

**Important**: Must be run from `.context-engine` directory.

---

## Prerequisites

1. **Curated rules**: Run `/ce-extract` and `/ce-tags-optimize-all` first
2. **Claude CLI**: Must be installed and authenticated
3. **Git repository**: Project should be a git repo for commit history

---

## Output

The pipeline produces:
- `../onboard-root.yaml` - Main onboarding artifact in project root
- `work/candidates/*.json` - Intermediate candidate sets (for debugging)
- `work/selections/*.yaml` - Claude's curation selections (preserved)

Output structure includes:
- Metadata (timestamp, version, pipeline info)
- Project summary (150-200 words)
- Rules organized by domain (tier_1_domains)
- Relationship cross-references
- Implementation pointers

---

## Using the Output

The generated `onboard-root.yaml` can be:
- Read by new Claude Code sessions for project context
- Referenced in CLAUDE.md for automatic loading
- Used for agent handoff documentation

---

## Troubleshooting

### Empty selections

If stages produce empty selections:
- Ensure database has curated rules (`tags_state != 'needs_tags'`)
- Run `/ce-tags-optimize-all` to tag untagged rules
- Check Claude CLI authentication

### Stage timeout

Individual Claude stages have 120s timeout:
- Pipeline continues with empty selections for that stage
- Re-run `/ce-onboard-generate` to retry
- Check network connectivity

### Missing onboard-root.yaml

Verify output path:
```bash
ls -la ../onboard-root.yaml
```

Check Stage 5 completed successfully in output.

---

✓ Ready for onboard generation!
