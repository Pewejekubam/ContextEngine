# Context Engine Makefile
# Makefile generation for workflow automation and CI/CD
#
# Generated from: specs/modules/build-config-makefile-wrapper-v1.3.2.yaml
# Generator: makefile_generator.py

.PHONY: help ci-pipeline chatlogs-extract chatlogs-debug chatlogs-validate tags-optimize tags-stats database-init database-status database-clean onboard-generate tags-optimize-auto tags-review tags-check

# Display available commands (default target)
help:
	@echo "Context Engine - Available Commands"
	@echo "===================================="
	@echo ""
	@echo "CI/CD Pipeline:"
	@echo "  ci-pipeline               Run full ETL pipeline (chatlogs-extract → tags-optimize → validate)"
	@echo ""
	@echo "Chatlog Workflow:"
	@echo "  chatlogs-extract          Extract rules from chatlogs to database (batch mode)"
	@echo "                            └─ Slash command: /ce-extract"
	@echo "  chatlogs-validate         Validate specific chatlog (requires FILE=path)"
	@echo "  chatlogs-debug            Debug all chatlogs with verbose JSON output"
	@echo "                            Note: Use /ce-capture to create new chatlogs"
	@echo ""
	@echo "Tags Workflow:"
	@echo "  tags-optimize             Optimize tags for all needs_tags rules (interactive)"
	@echo "  tags-optimize-auto        Automated batch optimization (non-interactive, CI/CD)"
	@echo "  tags-review               Monthly vocabulary review (typo/synonym cleanup)"
	@echo "  tags-check                Pre-commit health check"
	@echo "  tags-stats                Display tag usage histogram"
	@echo ""
	@echo "Database Workflow:"
	@echo "  database-init             Initialize fresh database from schema"
	@echo "  database-status           Show database statistics and rule counts"
	@echo "  database-clean            Remove and reinitialize database (preserves chatlogs)"
	@echo "                            Note: Use /ce-mark-salience for manual rule priority override"
	@echo ""
	@echo "Onboarding Workflow:"
	@echo "  onboard-generate          Generate agent onboarding context"

# Run full ETL pipeline (extract → optimize-tags → validate)
ci-pipeline:
	@echo "Running CI/CD Pipeline"
	@echo "======================"
	@echo ""
	@echo "Step 1: Extract rules from chatlogs..."
	@make chatlogs-extract
	@echo ""
	@echo "Step 2: Optimize tags (batch mode)..."
	@make tags-optimize
	@echo ""
	@echo "Step 3: Database validation..."
	@make database-status
	@echo ""
	@echo "✓ Pipeline complete"

# Extract rules from chatlogs to database (batch mode)
chatlogs-extract:
	@echo "Extracting rules from chatlogs..."
	@python3 scripts/extract.py

# Debug all chatlogs with deployment compatibility checks (CAP-040h, CAP-040i, CAP-040j)
chatlogs-debug:
	@echo "Debugging chatlogs (verbose JSON output with deployment compatibility checks)..."
	@for chatlog in data/chatlogs/*.yaml; do \
	  if [ -f "$$chatlog" ]; then \
	    echo "  Validating $$chatlog"; \
	    python3 scripts/validate_chatlog.py --debug "$$chatlog" || exit 1; \
	  fi; \
	done
	@echo ""
	@echo "✓ All chatlogs passed validation"

# Validate a specific chatlog file with deployment compatibility checks
chatlogs-validate:
ifndef FILE
	@echo "Usage: make chatlogs-validate FILE=<value>"
	@exit 1
endif
	@echo "Validating chatlog: $(FILE)"
	@python3 scripts/validate_chatlog.py --debug $(FILE)

# Optimize tags for all needs_tags rules (batch mode)
tags-optimize:
	@echo "Optimizing tags (batch mode)..."
	@python3 scripts/optimize-tags.py

# Display tag usage statistics histogram
tags-stats:
	@echo "Displaying tag usage statistics..."
	@python3 scripts/tags-stats.py

# Initialize fresh database from schema (no confirmation required)
database-init:
	@echo "Initializing database from schema..."
	@if [ -f data/rules.db ]; then \
	  echo "✗ Database already exists at data/rules.db" >&2; \
	  echo "  Use 'make database-clean' to remove and reinitialize" >&2; \
	  exit 1; \
	fi
	@if [ -f schema/schema.sql ]; then \
	  sqlite3 data/rules.db < schema/schema.sql; \
	  echo "✓ Database initialized at data/rules.db"; \
	else \
	  echo "✗ Error: schema/schema.sql not found" >&2; \
	  exit 1; \
	fi

# Show database statistics and rule counts
database-status:
	@echo "Database Statistics"
	@echo "==================="
	@if [ -f data/rules.db ]; then \
	  echo "Location: $$(pwd)/data/rules.db"; \
	  V=$$(sqlite3 data/rules.db "SELECT value FROM schema_metadata WHERE key='schema_version'" 2>/dev/null || echo unknown); echo "Schema version: $$V"; \
	  echo ""; \
	  sqlite3 data/rules.db "SELECT 'Total rules: ' || COUNT(*) FROM rules"; \
	  echo ""; \
	  echo "Rules by type:"; \
	  sqlite3 data/rules.db "SELECT '  ' || type || ': ' || COUNT(*) FROM rules GROUP BY type ORDER BY type"; \
	  echo ""; \
	  echo "Rules by tags_state:"; \
	  sqlite3 data/rules.db "SELECT '  ' || tags_state || ': ' || COUNT(*) FROM rules GROUP BY tags_state ORDER BY tags_state"; \
	  echo ""; \
	  sqlite3 data/rules.db "SELECT 'Total chatlogs: ' || COUNT(*) FROM chatlogs"; \
	else \
	  echo "Database not found: data/rules.db"; \
	  echo "Create with: make database-init, then populate with /ce-extract"; \
	fi

# Remove and reinitialize database (preserves chatlogs, requires confirmation)
database-clean:
	@echo "WARNING: This will delete and reinitialize the database (chatlogs will be preserved)"
	@echo -n "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
	@echo "Removing database..."
	@rm -f data/rules.db
	@echo "Reinitializing database from schema..."
	@if [ -f schema/schema.sql ]; then \
	  sqlite3 data/rules.db < schema/schema.sql; \
	  echo "✓ Database reinitialized. Populate with /ce-extract (or: make chatlogs-extract)"; \
	else \
	  echo "✗ Error: schema/schema.sql not found" >&2; \
	  exit 1; \
	fi

# Generate agent onboarding context YAML (5-stage pipeline)
onboard-generate:
	@echo "=== Context Engine: Onboard Pipeline ==="
	@echo "Stage 1: Generating candidates..."
	@python3 scripts/onboard/stage-1-candidates.py
	@echo "Stage 2: Curating foundational ADRs..."
	@bash scripts/onboard/stage-2-foundational.sh
	@echo "Stage 3: Curating recent activity..."
	@bash scripts/onboard/stage-3-recent.sh
	@echo "Stage 4: Summarizing project status..."
	@bash scripts/onboard/stage-4-summary.sh
	@echo "Stage 5: Assembling onboard-root.yaml..."
	@python3 scripts/onboard/stage-5-assembly.py
	@echo "✓ Generated: ../onboard-root.yaml"

# Automated batch optimization (non-interactive, CI/CD)
tags-optimize-auto:
	@echo "Running automated batch tag optimization..."
	@python3 scripts/optimize-tags.py --auto-approve

# Monthly vocabulary review (typo/synonym/rare tag cleanup)
tags-review:
	@echo "Running vocabulary review..."
	@python3 scripts/tags-review.py

# Pre-commit health check (untagged count, typo detection)
tags-check:
	@python3 scripts/tags-check.py
