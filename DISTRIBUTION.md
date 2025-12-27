# Distribution & Architecture

This document explains the architecture of Context Engine and the relationship between this repository and the build engine that generates it.

---

## What This Repository Is

**Context Engine** (this repository) is a **distribution package** for the Context Engine runtime—a portable, drop-in tool for capturing and managing engineering decisions.

This repository contains:
- ✅ **Generated artifacts** - Scripts, schemas, templates ready to deploy
- ✅ **Documentation** - User guides, examples, API documentation
- ✅ **Configuration** - Default settings and deployment templates
- ✅ **Community infrastructure** - Contributing guidelines, governance, security policy

This repository does NOT contain:
- ❌ **Source code generation** - The build engine that creates these artifacts
- ❌ **Tests** - Testing happens in the build engine project
- ❌ **Development tools** - Development infrastructure is separate

---

## Build Engine vs. Distribution Package

### Build Engine (Private)

**Responsibility:**
- Generate Context Engine scripts and artifacts
- Conduct testing and validation
- Manage versions and releases
- Create quality-assured packages

**What it produces:**
- Python scripts (`scripts/*.py`)
- Database schemas (`schema/*.sql`)
- Configuration templates (`config/*.yaml`)
- Prompt templates (`templates/*.md`)

**Access:** Internal development only

### Distribution Package (Public - This Repository)
**Location:** `https://github.com/Pewejekubam/ContextEngine`

**Responsibility:**
- Package generated artifacts for distribution
- Provide documentation and examples
- Manage community contributions
- Enable deployment and usage

**What it contains:**
- Generated artifacts (copied from build engine)
- User-facing documentation
- Installation instructions
- Community governance

**Access:** Public GitHub repository

---

## Release Process

```
┌─────────────────────────────────────────────────────────────────┐
│ Build Engine (Private)                                          │
│                                                                  │
│ 1. Generate artifacts                                           │
│ 2. Run comprehensive tests                                      │
│ 3. Validate across environments                                 │
│ 4. Create release package                                       │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ Copy verified artifacts
                     │
┌────────────────────▼────────────────────────────────────────────┐
│ Distribution Package (Public)                                   │
│                                                                  │
│ 1. Receive artifacts from build engine                          │
│ 2. Validate artifact integrity                                  │
│ 3. Update documentation                                         │
│ 4. Prepare for public distribution                              │
│ 5. Create GitHub release                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Why This Architecture?

### Separation of Concerns

**Build Engine** focuses on:
- Code generation correctness
- Implementation details
- Testing and validation
- Version control of specifications

**Distribution Package** focuses on:
- User experience
- Community engagement
- Documentation clarity
- Easy deployment

### Intellectual Property

The build engine represents significant intellectual property:
- Sophisticated code generation algorithms
- Domain-specific language implementation
- Optimization techniques
- Internal architecture patterns

**We release the artifacts** (what Context Engine does) **not the blueprint** (how we generate it).

This is similar to how other tools work:
- **Docker** - You get the container runtime, not the source code that built it
- **Kubernetes** - You get the orchestrator, not Google's internal tooling
- **TensorFlow** - You get the framework, not the research lab that created it

### Quality Assurance

Testing happens in the build engine where:
- All constraints and requirements are verified
- Edge cases are tested against specifications
- Performance is validated
- Compatibility is confirmed

This repository validates that artifacts are properly packaged and documented, not that the artifacts work (they were already validated).

---

## Validation in This Repository

Since this is a **distribution package**, not a development project, CI/CD focuses on:

✅ **Artifact Integrity**
- Python script syntax validation
- YAML configuration validation
- SQLite schema validation

✅ **Documentation Quality**
- Placeholder detection
- Link validation
- Consistency checks

✅ **Packaging Correctness**
- Makefile integrity
- README accuracy
- Configuration templates validity

❌ **NOT tested here:**
- Feature functionality (validated in build engine)
- Behavioral correctness (validated in build engine)
- Cross-platform compatibility (validated in build engine)

---

## Contributing

**All contributions are welcome in this repository!**

### You can contribute:

✅ **Code improvements** - Submit PRs to improve scripts, schemas, templates
- Maintainers can backport improvements to the build engine
- Your fixes and enhancements will be included in future releases

✅ **Documentation** - Improve guides, examples, clarity
- Better docs help everyone understand and use Context Engine

✅ **Examples** - Add use cases, scenarios, patterns
- Show others how to apply Context Engine effectively

✅ **Bug reports** - Report issues with deployed artifacts
- Help us identify and fix problems

✅ **Feature requests** - Suggest new capabilities
- Open discussions to explain your use case
- Maintainers evaluate feasibility and implementation path

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

---

## Transparency Note

We're being explicit about this architecture because:

1. **Respect your time** - If you want to modify Context Engine's core behavior, you need to know this isn't the right place
2. **Set expectations** - CI/CD here validates distribution, not features
3. **Clarify our offering** - We're giving you the production artifact, not the workshop

This is a normal OSS pattern, but we think it's better to be upfront about it rather than imply this is a traditional "development" repository.

---

## Questions?

- **How do I...?** → [FAQ.md](docs/FAQ.md)
- **I found a bug** → [GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues)
- **I have an idea** → [GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues/new?labels=enhancement&title=Idea:)
- **I want to contribute** → [CONTRIBUTING.md](CONTRIBUTING.md)

---

Last updated: November 2025
