# Upgrading Context Engine

This guide covers upgrading your existing Context Engine installation to a newer version.

---

## Compatibility Check

The setup script automatically checks schema compatibility when you run it.

**Before upgrading:**

1. Check your current version:
   ```bash
   sqlite3 data/rules.db "SELECT value FROM schema_metadata WHERE key='schema_version'"
   ```

2. Backup your database:
   ```bash
   cp data/rules.db data/rules.db.backup_$(date +%Y%m%d_%H%M%S)
   ```

3. Backup your configurations:
   ```bash
   cp config/deployment.yaml deployment.yaml.backup
   cp config/tag-vocabulary.yaml tag-vocabulary.yaml.backup
   ```

---

## Upgrade Process

Choose the upgrade method that matches your installation type.

### Method 1: Git Clone Upgrade

If you installed via `git clone`, upgrading is straightforward:

1. **Pull the latest changes:**
   ```bash
   cd .context-engine
   git pull origin main
   ```

2. **Run initialization to apply any updates:**
   ```bash
   Ask Claude: "Please process commands/ce-init.md"
   ```

   The initialization will:
   - Detect existing deployment.yaml (upgrade mode)
   - Check schema version compatibility
   - Preserve your configurations and data
   - Report migration requirements if versions incompatible

**Handling local customizations:**

If you've modified files tracked by git, you have several options:

- **Stash changes temporarily:**
  ```bash
  git stash
  git pull origin main
  git stash pop
  ```

- **Keep changes on a custom branch:**
  ```bash
  git checkout -b my-customizations
  # Make your changes
  git commit -m "Local customizations"

  # To upgrade later:
  git checkout main
  git pull origin main
  git checkout my-customizations
  git rebase main
  ```

- **Review conflicts manually:**
  ```bash
  git pull origin main
  # Resolve any conflicts in your editor
  git add .
  git commit -m "Merge upstream changes"
  ```

**Note:** Your `config/` directory files (deployment.yaml, tag-vocabulary.yaml) and `data/` directory are typically not tracked by git when you customize them, so they're preserved automatically.

---

### Method 2: Tarball Upgrade

If you installed via tarball, follow these steps.

#### Automated Upgrade with Initialization Prompt (Recommended)

The initialization prompt handles upgrades automatically:

1. Extract new version over your existing installation:
   ```bash
   tar -xf context-engine-runtime-v<new-version>.tar
   cd .context-engine
   ```

2. Run initialization (detects upgrade automatically):
   ```bash
   Ask Claude: "Please process commands/ce-init.md"
   ```

   The prompt will:
   - Detect existing deployment.yaml (upgrade mode)
   - Check schema version compatibility
   - Preserve your configurations and data
   - Update scripts and runtime files
   - Run `ce-init.sh --setup` to reconfigure paths
   - Report migration requirements if versions incompatible

#### Manual Tarball Upgrade (Fallback)

If you prefer manual control:

1. Download new version:
   ```bash
   tar -xf context-engine-runtime-v<new-version>.tar
   ```

2. Backup your data:
   ```bash
   cp .context-engine/data/rules.db rules.db.backup
   cp .context-engine/config/*.yaml config-backup/
   ```

3. Replace runtime files (keep your data):
   ```bash
   # Remove old scripts and schema
   rm -rf .context-engine/scripts .context-engine/schema .context-engine/Makefile

   # Copy new runtime files
   cp -r .context-engine-new/scripts .context-engine/
   cp -r .context-engine-new/schema .context-engine/
   cp -r .context-engine-new/commands .context-engine/
   cp .context-engine-new/Makefile .context-engine/
   ```

4. Review new example configs (don't overwrite yours):
   ```bash
   diff .context-engine/config/deployment.yaml .context-engine-new/config/deployment.yaml.example
   diff .context-engine/config/tag-vocabulary.yaml .context-engine-new/config/tag-vocabulary.yaml.example
   ```

5. Re-run setup to update paths:
   ```bash
   cd .context-engine
   bash commands/ce-init.sh --setup
   ```

---

## Version-Specific Migrations

### v2.0.0 → v2.1.0

**Changes:**
- Extract module now uses pure ETL (no tag normalization)
- All existing rules get `tags_state='needs_tags'`
- Optimize-tags module now vocabulary-aware

**Action Required:**
1. Re-extract existing chatlogs to update tags_state:
   ```bash
   make chatlogs-extract
   ```

2. Run tag optimization on all rules:
   ```bash
   make tags-optimize
   ```

### v1.x → v2.0.0

**Breaking Changes:**
- Modular spec architecture
- Configuration moved to `config/deployment.yaml`
- Database schema includes `schema_metadata` table

**Migration Steps:**
1. Export rules from v1.x:
   ```bash
   sqlite3 data/rules.db ".dump" > rules_v1_dump.sql
   ```

2. Install v2.0.0 in new directory

3. Manually migrate rules (schema changed - requires custom script)

---

## Rollback Procedure

If you encounter issues after upgrading, follow the rollback procedure for your installation type.

### Git Clone Rollback

1. **Check out the previous version:**
   ```bash
   cd .context-engine
   git log --oneline -10  # Find the commit before upgrade
   git checkout <previous-commit-hash>
   ```

   Or if you know the version tag:
   ```bash
   git checkout v3.3.0  # Replace with your previous version
   ```

2. **Restore your backed-up database** (if needed):
   ```bash
   cp rules.db.backup_<timestamp> .context-engine/data/rules.db
   ```

3. **Re-run setup:**
   ```bash
   bash commands/ce-init.sh --setup
   ```

4. **Return to main branch** when ready to try upgrading again:
   ```bash
   git checkout main
   ```

### Tarball Rollback

1. Stop using the new version

2. Restore your backed-up database:
   ```bash
   cp rules.db.backup_<timestamp> .context-engine/data/rules.db
   ```

3. Restore your backed-up configs:
   ```bash
   cp deployment.yaml.backup .context-engine/config/deployment.yaml
   cp tag-vocabulary.yaml.backup .context-engine/config/tag-vocabulary.yaml
   ```

4. Re-extract your previous version tarball

5. Run setup to reconfigure paths:
   ```bash
   cd .context-engine
   bash commands/ce-init.sh --setup
   ```

---

## Database Schema Migrations

Schema migrations are NOT automatic. Each version documents required schema changes.

**Check schema compatibility:**

```bash
# Database version
sqlite3 data/rules.db "SELECT value FROM schema_metadata WHERE key='schema_version'"

# Package version
grep -oP 'Context Engine Database Schema v\K[0-9.]+' schema/schema.sql
```

**If versions don't match:**
1. Check this guide for migration steps
2. Backup database before proceeding
3. Follow version-specific instructions above
4. Verify migration success

**If no migration path exists:**
- Contact maintainers
- Consider fresh installation with chatlog re-extraction
