# Maintainers

This document describes the maintenance and governance of the Context Engine project.

---

## Project Maintainers

### Primary Maintainer

**Pewejekubam**
- **Role:** Project creator and lead maintainer
- **Responsibilities:**
  - Overall project direction and strategy
  - Release management and versioning
  - Security vulnerability handling
  - Code review and quality standards
- **Contact:** [@Pewejekubam](https://github.com/Pewejekubam) on GitHub

---

## Response Times

**Maintenance Model:** Best Effort by Maintainer

All maintenance activities—including issue responses, PR reviews, security reports, and feature requests—are handled on a best-effort basis. Response times depend on maintainer availability and project priorities, but we're committed to addressing issues thoughtfully when time permits.

| Issue Type | Expectation |
|-----------|------------|
| All Issues | Best effort response and resolution |
| Security Vulnerabilities | Prioritized for prompt review per [SECURITY.md](SECURITY.md) |
| Community Discussions | Encouraged; may be answered by community members |

---

## Becoming a Maintainer

Context Engine is growing and we're actively looking for co-maintainers to help with:

- **Documentation** - Keep docs current and clear
- **Community Support** - Answer questions in Discussions and Issues
- **Examples** - Contribute and maintain real-world use case examples
- **Integration Building** - Create GitHub Actions, IDE plugins, etc.
- **Code Review** - Help review PRs and maintain code quality

### How to Get Involved

1. **Start Contributing**
   - Pick an issue labeled **`💚 good first issue`**
   - Submit PRs for features or improvements
   - Participate in Discussions

2. **Build Track Record**
   - Show sustained engagement over time
   - Demonstrate understanding of project goals
   - Exhibit good judgment in community interactions

3. **Discuss Maintainership**
   - Open a Discussion proposing your interest
   - Describe area you'd like to help with
   - Discuss scope and responsibilities

4. **Get Invited**
   - Primary maintainer will extend official invitation
   - Grant appropriate GitHub permissions
   - Add to MAINTAINERS.md

---

## Decision Making

### Project Decisions

**Small decisions** (documentation updates, minor bug fixes, non-breaking improvements):
- Can be made unilaterally by any active contributor after PR review

**Medium decisions** (new features, API changes, deprecations):
- Require discussion in GitHub Issues or Discussions
- Should have at least 1 week of community feedback
- Primary maintainer makes final decision

**Large decisions** (major version changes, project direction, breaking changes):
- Discussed extensively in Discussions
- Multiple contributors should weigh in
- Aim for community consensus
- Primary maintainer ensures alignment with project goals

### Roadmap

The [ROADMAP.md](ROADMAP.md) represents the current prioritized direction. Changes to roadmap:
- Proposed via GitHub Discussions
- Discussed with community
- Updated based on feedback and capacity

---

## Release Process

### Versioning

Context Engine follows [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 3.1.0)
- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes

### Release Steps

1. **Update CHANGELOG.md**
   - Document all changes since last release
   - Use [Keep a Changelog](https://keepachangelog.com/) format

2. **Version Bump**
   - Update version in:
     - `pyproject.toml`
     - README.md (in example installations)
     - Any version-specific docs

3. **Create GitHub Release**
   - Tag: `vX.Y.Z`
   - Title: `Context Engine vX.Y.Z`
   - Description: Copy from CHANGELOG

4. **Announce**
   - Post in Discussions under Announcements
   - Include summary of major changes
   - Highlight upgrade path if needed

---

## Code Standards

### Style Guidelines

- **Python:** Follow [PEP 8](https://pep8.org/)
- **Bash:** Use `shellcheck` for validation
- **YAML:** Validate syntax before committing
- **Documentation:** Clear, conversational, avoid jargon

### Review Checklist

Before merging a PR, ensure:

- [ ] Code follows style guidelines
- [ ] Tests pass (when applicable)
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No security concerns introduced
- [ ] Backwards compatible (unless intentionally breaking)
- [ ] Comments explain "why" not just "what"

### Testing

Currently, Context Engine uses:
- `make tags-check` - Health verification
- `make chatlogs-extract` - Functional testing
- Manual testing with fresh database

Future: Automated test suite planned for v3.2+

---

## Security

See [SECURITY.md](SECURITY.md) for:
- Vulnerability reporting process
- Security best practices
- Known considerations
- Response timeline

**Summary:**
- Do NOT open public issues for security vulnerabilities
- Report privately via GitHub Security Advisories
- We respond within 48 hours
- Fixes released within 30 days

---

## Deprecation Policy

When deprecating features:

1. **Announce** in CHANGELOG and docs
2. **Warn** - Feature still works but shows deprecation notice
3. **Grace period** - Minimum 1 minor version before removal
4. **Remove** - Feature removed in next major version

Example deprecation timeline:
- v3.1: Feature X deprecated, warning added
- v3.2: Feature X still works, stronger warning
- v4.0: Feature X removed entirely

---

## License & Attribution

- **License:** MIT (see [LICENSE](LICENSE))
- **Copyright:** Context Engine contributors
- **Attribution:** Include when using significant portions

---

## Contact & Questions

- **General Questions:** [GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues) (use `question` label)
- **Bug Reports:** [GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues)
- **Security Issues:** See [SECURITY.md](SECURITY.md)
- **Direct Contact:** Create an Issue

---

## Acknowledgments

Thank you to everyone who contributes to Context Engine—whether through code, documentation, examples, or community support. The project succeeds because of contributions from people like you.

---

Last updated: November 2025
