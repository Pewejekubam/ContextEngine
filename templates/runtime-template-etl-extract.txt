# Extract Chatlogs to Database

> Context Engine ETL: Transform captured chatlogs into database rules

## Overview

Extract rules from captured chatlogs and insert them into the Context Engine database.

**What this does:**
- Reads all chatlog YAML files from `data/chatlogs/`
- Transforms rules (ADRs, constraints, invariants) into database rows
- Assigns empty tags + `needs_tags` state for downstream optimization
- Reports extraction statistics

**When to use:**
- After capturing session knowledge with `/ce-capture`
- Before running tag optimization
- As first step in CI/CD pipeline

---

## Run Extraction

```bash
# Run from .context-engine directory
cd .context-engine
python3 scripts/extract.py
```

The Python script handles:
- Path detection from `deployment.yaml`
- Chatlog discovery and validation
- Database creation/updates
- Progress reporting
- Statistics summary
- Error handling

**Important**: Must be run from `.context-engine` directory for correct relative paths.

---

## Alternative: Use Make

```bash
make chatlogs-extract
```

Both methods produce identical results.

---

## Troubleshooting

### Error: "malformed chatlog YAML"

Validate the chatlog:
```bash
python3 scripts/validate_chatlog.py data/chatlogs/<filename>.yaml
```

### Error: "database is locked"

Close other processes accessing `rules.db` and retry.

### Error: "deployment.yaml not found"

Run `/ce-init` to initialize Context Engine first.

---

✓ Extraction ready!
