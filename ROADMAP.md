# Context Engine Roadmap

Public roadmap showing where Context Engine is headed and what's planned for future releases.

---

## Current Release: v3.6.0

**Status:** Stable, feature-complete for core use cases
**Release Date:** December 11, 2025
**Focus:** LLM-assisted conflict resolution and codebase refactoring

### What's New in v3.6.0

- **LLM-assisted conflict resolution** (Spec 77 v1.3.0)
  - New `llm_assisted` conflict resolution strategy
  - Confidence-gated auto-approval (default threshold: 0.80)
  - Cost limits and safety controls for LLM processing
  - New template: `runtime-template-rules-conflict-resolution.txt`
- **Configuration enhancements**
  - `auto_resolution:` section with fine-grained controls
  - Per-run cost limits and conflict caps
  - Timeout configuration for LLM calls
- **Codebase refactoring**
  - Significant cleanup across all Python scripts
  - Improved code organization and documentation
  - Removed template metadata headers from commands (cleaner output)
  - Streamlined deployment.yaml structure

### Core Features (All Versions)

✅ **Core Capture**
- Session knowledge capture via `/ce-capture` command
- YAML-based chatlog format with provenance
- Quality validation and warnings

✅ **Database & Querying**
- SQLite-based persistent knowledge store
- Four rule types (Decision, Constraint, Invariant, Pattern)
- Tag-based organization and discovery

✅ **AI-Powered Optimization**
- Claude-powered tag suggestion and refinement
- Human-in-the-loop approval workflow
- Vocabulary-aware recommendations

✅ **Knowledge Generation**
- Automated onboarding context generation
- Structured YAML output for new developers
- Integration with project documentation

✅ **Developer Experience**
- Claude Code slash commands (`/ce-capture`, etc.)
- Make-based workflow automation
- Comprehensive examples
- DRY, non-redundant documentation

✅ **Team Foundation**
- Code of Conduct (Contributor Covenant 2.1)
- Security policy with disclosure process
- Contributing guidelines
- Issue/PR templates

---

## Recent Releases

### v3.5.0 (December 8, 2025)

- **Automated rule curation** (Spec 77 v1.1.0) with CI/CD JSON output
  - Duplicate detection and merging
  - Low confidence archival (configurable threshold)
  - Domain migrations with history tracking
  - Conflict detection and resolution strategies
- New make commands: `rules-curate`, `rules-curate-dry-run`
- `curation:` configuration section in deployment.yaml
- Template metadata headers for version tracking

### v3.4.1 (December 4, 2025)

- Schema hardening and ETL refactoring
- Explicit behavior definitions in deployment config
- Standardized placeholder naming in vocabulary templates

### v3.4.0 (November 29, 2025)

- Integrated setup.sh into ce-init.sh for streamlined initialization
- Single CLI tool (`ce-init.sh --setup`) handles all deployment configuration
- Simplified installation workflow

### v3.3.0 (November 24, 2025)

- Repository cleanup and organization
- Fixed GitHub Release automation
- Documentation improvements

---

## Future Direction

We're exploring several directions for future development. These are areas of interest, not commitments:

### Team Collaboration
- Domain-specific onboarding generation
- Knowledge validation and quality gates
- ~~Conflict detection between rules~~ ✅ (v3.5.0)
- ~~LLM-assisted conflict resolution~~ ✅ (v3.6.0)
- Governance frameworks for teams

### Integration & Ecosystem
- Multi-AI assistant support (OpenAI, local models)
- CI/CD integration (GitHub Actions)
- Plugin architecture for extensions

### Scale & Accessibility
- Optional cloud sync capabilities
- Web-based interface for visualization
- Enhanced collaboration features

### Advanced Features
- Knowledge graph visualization
- IDE plugins
- Enterprise features (RBAC, SSO, audit logging)

---

## What We're NOT Planning

Some things intentionally out of scope:

❌ **Code generation** - Context Engine documents decisions, doesn't generate code
❌ **Full Git history integration** - We track decisions, not code changes
❌ **Automatic decision extraction** - Human judgment is essential (intentional design choice)
❌ **Replace version control** - Complements git, doesn't replace it
❌ **AI-only operation** - Always human-in-the-loop for important decisions

---

## Development Principles

These guide all roadmap decisions:

### 1. **Local-First**
Knowledge stays in your project. Cloud is optional, not required.

### 2. **Human-in-the-Loop**
AI assists, but humans make decisions. We don't auto-generate knowledge.

### 3. **Backwards Compatible**
New releases won't break existing knowledge bases or workflows.

### 4. **Open & Transparent**
This roadmap is public. Decisions are discussed openly.

### 5. **Pragmatic**
We ship working features, not perfect ones. Iterate based on feedback.

### 6. **Community-Driven**
Listen to users. If many people want feature X, we prioritize it.

---

## How to Influence the Roadmap

### Suggest Features

1. **Check if already planned** - Read this roadmap first
2. **Open discussion** - [GitHub Discussions](https://github.com/Pewejekubam/ContextEngine/discussions)
3. **Explain your use case** - Show the problem you're trying to solve
4. **Community upvoting** - Others can +1 ideas they want

### Contribute

Want to work on features?

1. **Open an issue** with implementation plan
2. **Discuss approach** with maintainers
3. **Submit PR** when ready

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### Become a Maintainer

As the project grows, we're looking for:
- Documentation maintainers
- Example contributors
- Integration builders
- Community advocates

Reach out on [Discussions](https://github.com/Pewejekubam/ContextEngine/discussions) if interested.

---

## Feedback & Questions

- **Want to discuss roadmap?** → [GitHub Discussions](https://github.com/Pewejekubam/ContextEngine/discussions)
- **Found a bug?** → [GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues)
- **Want to contribute?** → [CONTRIBUTING.md](CONTRIBUTING.md)

---

Last updated: December 2025 | [Discussions](https://github.com/Pewejekubam/ContextEngine/discussions) | [Issues](https://github.com/Pewejekubam/ContextEngine/issues)
