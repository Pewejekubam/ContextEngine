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

### Automated Upgrade with Initialization Prompt (Recommended)

The initialization prompt handles upgrades automatically:

1. Download and extract new version over your existing installation:

   **Download latest release automatically:**
   ```bash
   curl -s https://api.github.com/repos/Pewejekubam/ContextEngine/releases/latest | grep "browser_download_url.*tar" | cut -d '"' -f 4 | wget -qi -
   ```

   **Or download a specific version:**

   Using curl:
   ```bash
   curl -LO https://github.com/Pewejekubam/ContextEngine/releases/download/v<new-version>/context-engine-runtime-v<new-version>-<timestamp>.tar
   ```

   Using wget:
   ```bash
   wget https://github.com/Pewejekubam/ContextEngine/releases/download/v<new-version>/context-engine-runtime-v<new-version>-<timestamp>.tar
   ```

   Extract and enter directory:
   ```bash
   tar -xf context-engine-runtime-*.tar
   cd .context-engine
   ```

2. Run initialization (detects upgrade automatically):

   Ask Claude: "Please process .context-engine-init.md"

   The prompt will:
   - Detect existing deployment.yaml (upgrade mode)
   - Check schema version compatibility
   - Preserve your configurations and data
   - Update scripts and runtime files
   - Run setup.sh if needed
   - Report migration requirements if versions incompatible

### Manual Upgrade (Fallback)

If you prefer manual control:

1. Download and extract new version:

   **Download latest release automatically:**
   ```bash
   curl -s https://api.github.com/repos/Pewejekubam/ContextEngine/releases/latest | grep "browser_download_url.*tar" | cut -d '"' -f 4 | wget -qi -
   ```

   **Or download a specific version:**

   Using curl:
   ```bash
   curl -LO https://github.com/Pewejekubam/ContextEngine/releases/download/v<new-version>/context-engine-runtime-v<new-version>-<timestamp>.tar
   ```

   Using wget:
   ```bash
   wget https://github.com/Pewejekubam/ContextEngine/releases/download/v<new-version>/context-engine-runtime-v<new-version>-<timestamp>.tar
   ```

   Extract:
   ```bash
   tar -xf context-engine-runtime-*.tar
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
   cp .context-engine-new/Makefile .context-engine/
   cp .context-engine-new/setup.sh .context-engine/
   ```

4. Review new example configs (don't overwrite yours):
   ```bash
   diff .context-engine/config/deployment.yaml .context-engine-new/config/deployment.yaml.example
   diff .context-engine/config/tag-vocabulary.yaml .context-engine-new/config/tag-vocabulary.yaml.example
   ```

5. Re-run setup to update paths:
   ```bash
   cd .context-engine
   ./setup.sh
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

If you encounter issues after upgrading:

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

5. Run setup.sh to reconfigure paths:
   ```bash
   cd .context-engine
   ./setup.sh
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
