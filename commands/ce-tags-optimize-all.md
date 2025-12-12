# Automated Batch Tag Optimization

> Context Engine: Optimize tags for all untagged rules (non-interactive)


## Overview

Run automated batch tag optimization on all rules with `tags_state='needs_tags'`.

**What this does:**
- Processes all untagged rules through Claude for tag suggestions
- Auto-approves tags meeting confidence (≥0.70) and coherence (≥0.30) thresholds
- Updates vocabulary with approved tags (organic growth)
- Runs multi-pass iterations until convergence
- Reports progress and final statistics

**When to use:**
- After extracting rules with `/ce-extract`
- For bulk tagging of new rule sets
- As part of CI/CD pipeline automation

---

## Run Optimization

```bash
# Run from .context-engine directory
cd .context-engine
make tags-optimize-auto
```

The command executes multi-pass optimization:
1. Queries rules with `tags_state='needs_tags'`
2. Invokes Claude for each rule to suggest tags
3. Auto-approves tags meeting quality thresholds
4. Updates vocabulary with new approved tags
5. Repeats until convergence or cost limit

**Important**: Must be run from `.context-engine` directory.

---

## Prerequisites

1. **Database populated**: Run `/ce-extract` first to import rules from chatlogs
2. **Claude CLI**: Must be installed and authenticated (`claude --version`)
3. **Vocabulary file**: `config/tag-vocabulary.yaml` must exist

---

## Output

Progress output shows:
- Rule-by-rule processing status (✓ approved, ⊘ skipped, ✗ error)
- Confidence and coherence scores
- Claude's reasoning for tag suggestions
- Pass summaries with approval rates
- Vocabulary growth metrics

Final summary reports:
- Total rules processed
- Approval rate percentage
- New tags added to vocabulary
- Convergence status

---

## Troubleshooting

### No rules to optimize

```
No rules require tag optimization.
```

Run `/ce-extract` to populate database with rules from chatlogs.

### Low approval rate

If many rules are skipped:
- Check vocabulary has tier-2 tags for your domains
- Review skipped rules manually with `make tags-optimize` (interactive mode)
- Consider lowering confidence threshold in `config/build-constants.yaml`

### Claude CLI errors

Ensure Claude CLI is installed and authenticated:
```bash
claude --version
claude "test" --print
```

---

✓ Ready for batch optimization!
