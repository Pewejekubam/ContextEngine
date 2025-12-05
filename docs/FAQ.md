# Frequently Asked Questions (FAQ)

Common questions about Context Engine, how to use it, and troubleshooting.

---

## Getting Started

### Q: What is Context Engine?

A: Context Engine is a portable, drop-in tool for capturing and managing engineering decisions, constraints, and architectural knowledge across projects. It helps teams document the "why" behind their technical choices, not just the "what."

**Key capability:** Transform unstructured session knowledge into queryable, structured knowledge that onboards new developers faster.

### Q: Do I need Context Engine?

A: Context Engine is valuable for **almost all projects**:

- **Solo developers** - Capture decisions you'll forget later. When you return to the code after months, you'll remember WHY you made choices
- **Small teams** - New team members understand the reasoning, not just the implementation
- **Large teams** - Scale knowledge across teams and projects
- **Long-lived projects** - Decisions fade from memory. Document them when fresh
- **Short projects** - Still valuable! Captures patterns and constraints discovered
- **Complex systems** - Essential for multi-domain, multi-service architecture
- **Simple projects** - Even "obvious" decisions aren't obvious to future developers or your future self

**The worst time to learn why a decision was made is when you need to change it and don't understand the tradeoffs.**

**Best for any project where:**
- You want to remember your own decisions (sooner or later, you won't)
- Others need to understand your architecture (even if just your future self)
- You're building knowledge that compounds over time

### Q: What's the learning curve?

A: **Low!** Most developers are productive within 30 minutes:
1. Install (5 min)
2. Initialize (5 min)
3. Complete first session and capture (20 min)
4. Extract and query (5 min)

See [Getting Started](GETTING_STARTED.md) for guidance.

### Q: Can I use Context Engine with an existing project?

A: **Yes!** Context Engine works equally well with:
- New greenfield projects (start capturing immediately)
- Existing mature projects (capture new decisions going forward)

You don't need to retrofit all past decisions—focus on new knowledge as projects evolve.

---

## Installation & Setup

### Q: Git clone or tarball—which should I use?

A: **Use git clone** (recommended) unless you have a specific reason for tarball:

| Method | When to Use | Benefits |
|--------|-----------|----------|
| **Git Clone** | Default choice | Version-controlled, easy updates, track changes |
| **Tarball** | Air-gapped environments, offline deployment | Single file, no git required |

See [Getting Started - Installation](GETTING_STARTED.md#installation) for detailed instructions.

### Q: What if I want to update Context Engine later?

A: If you used **git clone:**
```bash
cd .context-engine
git pull origin main
```

Your configuration in `config/` is automatically preserved.

If you used **tarball:** Extract new version and manually migrate configuration files.

### Q: Where does Context Engine store my data?

A: All data stays in your project:
```
.context-engine/
├── data/
│   ├── rules.db          ← SQLite database (your knowledge)
│   └── chatlogs/         ← YAML files (captured sessions)
├── config/               ← Your configuration
└── [scripts, templates, etc.]
```

**No cloud, no external services.** Everything is local and under your version control.

### Q: Can I back up my knowledge?

A: **Yes!** Your knowledge base is simple files:
1. **Database backup:** Copy `data/rules.db`
2. **Chatlogs backup:** Commit `data/chatlogs/` to git
3. **Configuration backup:** Commit `config/` to git

**Best practice:** Keep `.context-engine` in version control (git track all files).

---

## Capturing Knowledge

### Q: When should I capture knowledge?

A: Capture after **meaningful development sessions** (30 min - few hours):
- ✅ Architectural decisions made
- ✅ Constraints or requirements discovered
- ✅ Patterns or solutions developed
- ✅ Integration points designed

**Don't capture:**
- ❌ Bug fixes (implementation detail)
- ❌ Small refactoring
- ❌ Tool configuration
- ❌ Things obvious from code

**Rule of thumb:** If a future developer needs to know **why** you made a choice, capture it.

### Q: What goes into a capture?

A: Three types of knowledge:

1. **Decisions (ADR)** - Why we chose X over Y, tradeoffs considered
2. **Constraints (CON)** - Limitations, requirements, system boundaries
3. **Invariants (INV)** - Things that must always be true

See [Getting Started](GETTING_STARTED.md) for detailed definitions.

### Q: How detailed should my captures be?

A: **Balance is important:**

**Too brief:**
```
Decision: Use Redis
Reason: It's fast
```
❌ No context for future developer

**Too detailed:**
```
Decision: Use Redis. We benchmarked 47 cache options...
[5 pages of benchmarks]
```
❌ Overkill, maintenance burden

**Just right:**
```
Decision: Use Redis for session caching

Rationale: Benchmarks showed 10x better performance than Memcached
for our access patterns. Tradeoff: Redis requires more memory than
Memcached, but storage costs are acceptable.

Constraint: Must use Redis 6.0+ for ACL support (needed for multitenancy)
```
✅ Explains why, acknowledges tradeoffs, documents constraints

### Q: Can I edit captures after creating them?

A: **Yes!** Chatlogs are YAML files:
```bash
vim .context-engine/data/chatlogs/[filename].yaml
```

Edit freely, but keep YAML syntax valid. Run validation after editing:
```bash
make chatlogs-validate FILE=.context-engine/data/chatlogs/[filename].yaml
```

### Q: What if I capture something wrong?

A: **No problem.** Options:
1. **Edit the chatlog** - Fix YAML directly and re-extract
2. **Delete the chatlog** - Remove the file and re-capture
3. **Tag for review** - Add "needs-review" tag and fix later

Your database is queryable, so you can always audit and correct bad data.

---

## Extracting & Querying

### Q: How often should I extract chatlogs?

A: **Whenever you want updated knowledge in the database:**
```bash
make chatlogs-extract
```

Typical workflow:
- After each capture session: Extract immediately
- Or batch: Extract weekly/monthly
- Or automated: Set up CI/CD to extract on PR merge

### Q: Can I query my knowledge?

A: **Yes!** It's a standard SQLite database:

```bash
# View all decisions
sqlite3 .context-engine/data/rules.db \
  "SELECT title, description FROM rules WHERE type='ADR';"

# Search by tag
sqlite3 .context-engine/data/rules.db \
  "SELECT * FROM rules WHERE tags LIKE '%performance%';"

# Count rules by type
sqlite3 .context-engine/data/rules.db \
  "SELECT type, COUNT(*) FROM rules GROUP BY type;"
```

Learn SQL basics to craft custom queries for your needs.

### Q: What if extraction fails?

A: Debug the chatlog:
```bash
make chatlogs-validate FILE=.context-engine/data/chatlogs/[filename].yaml
```

Common issues:
- YAML syntax error (missing colon, bad indentation)
- Invalid rule type (must be ADR, CON, or INV)
- Missing required fields (id, type, title)

Fix the YAML and re-extract.

---

## Tags & Organization

### Q: How do I tag knowledge well?

A: Use your project's **tier-1 domains** and **tier-2 tags**:

```yaml
# Good: Specific and discoverable
tags:
  - architecture      # tier-1: cross-cutting
  - database          # tier-1: domain
  - schema-design     # tier-2: database-specific
  - performance       # tier-2: database-specific
```

```yaml
# Bad: Too vague, hard to find later
tags:
  - important
  - stuff
  - note
```

See `config/tag-vocabulary.yaml` for your project's valid tags.

### Q: Can I optimize tags automatically?

A: **Yes!** Claude-powered tag optimization:

```bash
make tags-optimize-auto
```

Claude reviews your rules and suggests better tags in automated batch mode. For interactive approval/rejection of each suggestion, use `make tags-optimize` instead.

### Q: What if I want to change my vocabulary?

A: Edit `config/tag-vocabulary.yaml`:

```yaml
tier_1_domains:
  - frontend
  - backend
  - database

tier_2_tags:
  frontend:
    - react
    - performance
    - browser-compatibility
  # ... etc
```

Changes apply to future captures. Existing rules keep their tags until you re-optimize.

---

## Onboarding & Knowledge Sharing

### Q: How do I use captured knowledge for onboarding?

A: Generate onboarding context:

```bash
make onboard-generate
```

This creates `onboard-root.yaml` with your accumulated knowledge, formatted for new developers to read.

Share this with:
- New team members (first day)
- PRs documenting architectural changes
- Release notes explaining breaking changes
- Project wikis and documentation

### Q: Can I generate domain-specific onboarding?

A: **Planned feature!** Currently generates project-wide onboarding. Future versions will support:
```bash
make onboard-generate DOMAIN=frontend
```

For now, manually filter the generated YAML or use SQL queries to extract domain-specific knowledge.

### Q: What if my team doesn't use Context Engine?

A: Context Engine still provides value:
- **Solo developer:** Captures your own learning, helps you remember decisions
- **One adopter in team:** Share generated onboarding with colleagues, show the value
- **Growing adoption:** Start with individuals, scale to team practices (see Example 3)

Start with yourself; adoption often follows as value becomes visible.

---

## Team & Scaling

### Q: How do I set up Context Engine for a team?

A: Key practices for team adoption:
- Shared vocabulary across projects
- Code review integration
- Knowledge governance
- Team practices and discipline

**Quick summary:**
1. Create shared `team-vocabulary.yaml`
2. Each project inherits it
3. Integrate captures into code review
4. Tech lead reviews both code and knowledge

### Q: Can multiple projects share knowledge?

A: **Yes!** Two approaches:

**Approach 1: Shared vocabulary**
- Create `team-vocabulary.yaml` with common domains
- Each project's Context Engine inherits it
- Each project maintains its own database

**Approach 2: Centralized database**
- Share a single `rules.db` across projects (via git)
- Add "project" tag to identify origin
- Query across all projects

### Q: How do I prevent knowledge pollution?

A: Use **context hygiene** discipline:

```bash
# After initialization or tool maintenance
/clear

# Then work on your project
[meaningful development]

# Then capture
/ce-capture
```

This prevents Context Engine internals from polluting your project knowledge.

See [Getting Started - Context Hygiene](GETTING_STARTED.md#context-hygiene) for details.

---

## CI/CD & Automation

### Q: Can I integrate Context Engine into CI/CD?

A: **Yes!** You can automate:
- Chatlog validation on PR
- Automated extraction when PRs merge
- Knowledge metrics reporting

**Quick summary:** Add workflow that runs `make chatlogs-extract` when PRs merge.

### Q: What if a PR introduces conflicting knowledge?

A: You can configure CI to detect conflicts:
- Rule contradicts existing rule
- Decision contradicts code
- Constraint is already violated

### Q: Can I track knowledge metrics?

A: **Yes!** Use `make tags-stats` and `make database-status` for:
- Rules captured (by type)
- Tag distribution
- Tags state breakdown

---

## Troubleshooting

### Q: "Claude Code not found" error

A: Context Engine requires Claude Code CLI for `/ce-capture` and related slash commands.

**Solution:**
1. Install Claude Code: See [README](../README.md#requirements)
2. Ensure you're running commands in Claude Code (not regular terminal)

### Q: Database seems corrupted

A: First, don't panic—SQLite is robust. Try:

```bash
# Check integrity
sqlite3 .context-engine/data/rules.db "PRAGMA integrity_check;"

# If corrupted, restore from backup
rm .context-engine/data/rules.db
# (restore from backup or re-extract from chatlogs)
```

**Prevention:** Keep `.context-engine` in git, so you always have history.

### Q: Chatlogs won't extract

A: Debug step by step:

```bash
# Validate syntax
make chatlogs-validate FILE=.context-engine/data/chatlogs/[filename].yaml

# Check database
make database-status

# Try extraction with verbose output
make chatlogs-extract
```

Common issues: YAML syntax, invalid rule type, missing required fields.

### Q: Tags are a mess, hard to find things

A: Run tag optimization:

```bash
make tags-optimize
```

Claude reviews all rules and suggests consistent tagging. You can approve/reject each suggestion (interactive mode).

For automated batch mode without interactive approval, use:
```bash
make tags-optimize-auto
```

### Q: How do I reset everything and start over?

A: **Warning: This is destructive!**

```bash
# Backup first
cp -r .context-engine .context-engine.backup

# Delete database and chatlogs
rm .context-engine/data/rules.db
rm -r .context-engine/data/chatlogs

# Re-run initialization
# Ask Claude: "Please process .context-engine/commands/ce-init.md"
```

### Q: Performance is slow

A: For large databases (1000+ rules):

```bash
# Optimize database
sqlite3 .context-engine/data/rules.db "VACUUM;"

# Check if indexes are present
sqlite3 .context-engine/data/rules.db "PRAGMA index_info(idx_tags);"

# If missing, re-run extraction
make chatlogs-extract
```

---

## Contributing & Support

### Q: How do I report a bug?

A: Use [GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues) with:
- What you were trying to do
- Expected behavior
- Actual behavior
- System info (Python version, OS)

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

### Q: Can I contribute?

A: **Yes!** See [CONTRIBUTING.md](../CONTRIBUTING.md) for:
- How to report issues
- How to submit PRs
- Code of conduct
- Development setup

**Easy contributions:**
- Bug fixes
- Documentation improvements
- Example additions (show your use case!)
- Test coverage
- Feature suggestions (open issue first to discuss)

### Q: Where can I ask questions?

A: Three options:

1. **[GitHub Discussions](https://github.com/Pewejekubam/ContextEngine/discussions)** - Q&A, ideas, use cases
2. **[GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues)** - Bug reports, feature requests
3. **[FAQ](FAQ.md)** (you're reading it!) - Common questions

### Q: How do I stay updated?

A: Options:
- **Watch repository** on GitHub (get notifications)
- **Follow releases** (release notes with changelog)
- **Check ROADMAP.md** (see what's planned)

---

## Philosophy

### Q: Why does Context Engine exist?

A: Most engineering knowledge lives in people's heads or scattered in Slack messages. When people leave, get sick, or move to other projects, that knowledge vanishes.

Context Engine makes **important decisions, constraints, and patterns** queryable and shareable, so:
- New developers understand the **why**, not just the **what**
- Teams make consistent decisions (don't re-argue the same choices)
- Knowledge compounds (each session adds to the base)
- Onboarding is faster (context, not mystery)

### Q: What about my specific AI assistant requirement?

A: Context Engine **currently requires Claude Code** because that's what the developer uses and can thoroughly test.

**Future:** Modular architecture planned to support OpenAI, local models, etc. (see [ROADMAP.md](../ROADMAP.md)).

For now, Claude Code integrates seamlessly with slash commands (`/ce-capture`, etc.) and provides the best user experience.

---

## More Information

- **[Getting Started](GETTING_STARTED.md)** - Installation and first workflow
- **[ROADMAP.md](../ROADMAP.md)** - What's planned
- **[Contributing](../CONTRIBUTING.md)** - How to contribute
- **[Main README](../README.md)** - Project overview
