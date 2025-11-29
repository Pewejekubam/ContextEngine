# Changelog

All notable changes to Context Engine will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.4.0] - 2025-11-29

### Changed
- Integrated setup.sh functionality into ce-init.sh (Spec 71 v1.1.0)
- ce-init.sh `--setup` mode now handles all deployment configuration
- Streamlined initialization workflow with single CLI tool

### Removed
- setup.sh (functionality merged into ce-init.sh)

## [3.3.0] - 2025-11-24

### Changed
- Repository cleanup (removed nested .context-engine/ and .claude/ directories)
- Fixed GitHub Release script to handle existing releases gracefully

### Fixed
- GitHub Release automation now creates releases via API when gh CLI unavailable
- Installation instructions and documentation URLs corrected

## [3.2.0] - 2025-11-18

### Added
- Initial public release
- Comprehensive documentation suite
- CODE_OF_CONDUCT.md (Contributor Covenant v2.1)
- SECURITY.md with vulnerability reporting process
- CONTRIBUTING.md with contribution guidelines
- Community infrastructure (issue templates, PR templates)

## [3.1.0] - 2025-11-16

### Added
- Flattened repository structure for cleaner distribution
- Claude Code slash command integration (`/ce-capture`, `/ce-extract`, `/ce-init`, `/ce-mark-salience`)
- Workflow automation via Makefile
- SQLite database schema v3.1
- Tag vocabulary system with tier-1 and tier-2 classification
- Knowledge-driven development documentation
- Mermaid workflow diagram in README

### Changed
- Repository restructured from `.context-engine/` subdirectory to root-level artifacts
- Installation method simplified for git clone deployment
- Documentation updated for public OSS release
- Initialization process streamlined

### Security
- Added .gitignore for sensitive data (chatlogs, database, internal artifacts)
- Removed binary artifacts from git tracking

## [3.0.0] - 2024-XX-XX

### Added
- Initial internal release
- Chatlog capture and extraction system
- SQLite database backend
- Tag optimization with AI reasoning
- Onboarding context generation

### Technical Details
- Python 3.8+ support
- SQLite 3 schema
- PyYAML dependency
- Claude Code CLI integration

---

## Version History Notes

- **v3.4.0**: Streamlined initialization (setup.sh → ce-init.sh)
- **v3.3.0**: Repository cleanup and release automation fixes
- **v3.2.0**: Initial public release with full documentation
- **v3.1.0**: Public release preparation, repository restructuring
- **v3.0.0**: Initial internal development version
- **v2.x**: Legacy versions (unsupported)

## Migration Guides

For upgrading between versions, see [docs/UPGRADE.md](docs/UPGRADE.md).

---

[Unreleased]: https://github.com/Pewejekubam/ContextEngine/compare/v3.4.0...HEAD
[3.4.0]: https://github.com/Pewejekubam/ContextEngine/compare/v3.3.0...v3.4.0
[3.3.0]: https://github.com/Pewejekubam/ContextEngine/compare/v3.2.0...v3.3.0
[3.2.0]: https://github.com/Pewejekubam/ContextEngine/compare/v3.1.0...v3.2.0
[3.1.0]: https://github.com/Pewejekubam/ContextEngine/releases/tag/v3.1.0
