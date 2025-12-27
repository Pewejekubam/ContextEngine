# Community Engagement Guide

How to use GitHub Discussions, issue labels, and community features to build a healthy Context Engine community.

---

## GitHub Discussions

### What is GitHub Discussions?

GitHub Discussions is a community forum built into GitHub for conversations that aren't issues (bug reports or feature requests). It's ideal for:

- **Q&A** - "How do I...?" questions
- **Ideas** - "Has anyone thought about...?"
- **Announcements** - New versions, updates
- **Show & Tell** - Share your Context Engine use cases
- **General Discussion** - Thoughts, feedback, experiences

### Enabling Discussions

**For repository maintainers:**

1. Go to repository **Settings**
2. Scroll to **Features** section
3. Check **Discussions** checkbox
4. Click **Save**

GitHub will automatically create default discussion categories:
- **Announcements** - Important updates (moderator-only posting)
- **General** - General conversation
- **Ideas** - Feature suggestions and brainstorming
- **Polls** - Quick feedback surveys
- **Q&A** - Questions and answers (threaded responses)
- **Show and Tell** - Share your projects

### Using Discussions as a User

**Ask a Question:**
1. Click **Discussions** tab in repository
2. Click **New discussion**
3. Select **Q&A** category
4. Title: "How do I use Context Engine with multiple services?"
5. Describe your situation
6. Community members (and maintainers) respond

**Share Your Use Case:**
1. Select **Show and Tell** category
2. Describe your project and how you're using Context Engine
3. Include: What problem it solved, what you learned, recommendations
4. Great for building community and discovering use patterns

**Suggest an Idea:**
1. Select **Ideas** category
2. Title: "Domain-specific onboarding would help our team"
3. Explain the use case and benefit
4. Community votes (👍 emoji) on priority

---

## Issue Labels Strategy

### Why Labels Matter

Labels help users:
- **Find opportunities to contribute** ("good first issue")
- **Know what help is needed** ("help wanted")
- **Navigate issue tracker** ("bug" vs "feature" vs "documentation")

Labels help maintainers:
- **Triage efficiently** (know what issues are about)
- **Track work** (what's in progress, what's blocked)
- **Plan releases** (know what's done/not done)

### Recommended Label Structure

#### Priority Labels
```
🔴 priority/critical   - Breaks production, urgent security issue
🟠 priority/high       - Important, affects many users
🟡 priority/medium     - Good to have, doesn't block usage
🔵 priority/low        - Nice to have, can wait
```

#### Type Labels
```
🐛 type/bug             - Something is broken
✨ type/feature         - New capability
📚 type/documentation   - Docs, examples, guides
🔨 type/refactor        - Code quality, no behavior change
🧪 type/test            - Test coverage improvements
⚡ type/performance     - Speed, efficiency improvements
```

#### Difficulty Labels (For Contributors)
```
💚 good first issue     - Perfect for first-time contributors
🤝 help wanted          - Looking for contributor input
🎓 learning             - Requires understanding a subsystem
```

#### Status Labels
```
📋 status/triage        - Needs initial review
🔍 status/investigating - Maintainer is looking into it
✋ status/blocked        - Waiting on something else
🔄 status/in-progress   - Someone is working on it
✅ status/done          - Complete, waiting for release
👀 status/review        - PR open, needs review
```

#### Topic Labels (Area of Code)
```
📖 area/documentation  - Docs, examples, guides
🎯 area/examples       - Example projects
📝 area/cli            - CLI commands
💾 area/database       - Database/storage
🏷️  area/tags          - Tag vocabulary, organization
🤖 area/ai             - AI/Claude integration
🔧 area/configuration  - Setup, config files
```

### Creating a "Good First Issue"

When creating an issue good for new contributors:

1. **Be specific** - Exactly what needs to be done
2. **Provide context** - Why is this important?
3. **Add links** - Point to relevant code or docs
4. **Estimate effort** - "This should take ~2 hours"
5. **Label it** - Add `💚 good first issue`
6. **Offer help** - "Ask if you have questions!"

**Example:**

```markdown
## Title: Add example for authentication-based capture

### What
Create a new example showing how to capture decisions
when designing an auth system (OAuth, OIDC, JWT, etc.).

### Where
Add to `docs/examples/05-auth-system/` (new directory)

### What it should cover
- System design decisions (OAuth vs SAML vs custom)
- Token management constraints
- Security invariants
- Integration patterns with existing systems

### Related
- Follows pattern of Example 1-4
- Helpful for teams working on auth systems
- Requested in #42

### Effort estimate
~4-6 hours (research + writing)

### Questions?
Ask here or in Discussions if you need context!

**Labels:** 💚 good first issue, 📚 type/documentation, 🎯 area/examples
```

### Issue Triage Process

**For maintainers reviewing new issues:**

1. **Add type label** - Is this a bug, feature, doc, etc.?
2. **Add priority label** - How urgent is this?
3. **Add area label** - What part of the code?
4. **Respond to reporter** - Thank them, clarify if needed
5. **If it's a good first issue** - Label it and describe explicitly

**Good first issue criteria:**
- ✅ Clear, specific problem
- ✅ Limited scope (can't touch too many files)
- ✅ Doesn't require deep system knowledge
- ✅ Has good test case
- ✅ Provides learning opportunity

---

## Community Response Guidelines

### For Maintainers Responding to Issues

**Be welcoming:**
- Thank users for reporting
- Acknowledge the problem
- Show you're taking it seriously

**Be clear:**
- Explain what you found/need
- Ask for specific additional info
- Provide next steps

**Be helpful:**
- Link to relevant code
- Suggest approaches to fix
- Offer to pair on solution if stuck

**Be timely:**
- Respond within 48 hours if possible
- Update if things change
- Close when resolved

### For Contributors Asking Questions

**Use Discussions for:**
- Setup questions
- Usage questions
- General feedback
- Brainstorming ideas

**Use Issues for:**
- Bug reports (something broken)
- Feature requests (new capability)
- Documentation gaps

**Provide context:**
- What are you trying to do?
- What did you expect?
- What actually happened?
- How to reproduce?

---

## Building Community

### Share Your Story

Help others learn by sharing your experience:

1. **Use case** - How you're using Context Engine
2. **Problem it solved** - What was hard before?
3. **Lessons learned** - What you discovered
4. **Recommendations** - What would help others?

**Where to share:**
- [GitHub Issues - Share Your Use Case](https://github.com/Pewejekubam/ContextEngine/issues/new?labels=show-and-tell&title=Use%20Case:)
- Blog posts (link in your issue)
- Twitter/social media (mention @ContextEngine)
- Conference talks, podcasts, etc.

### Contribute to Examples

Examples are crucial for adoption. Ways to help:

**Submit your own example:**
- Show a use case not covered yet
- Follow the template from existing examples
- Include walkthrough, sample files, reference outputs

**Improve existing examples:**
- Add clarifications
- Fix errors
- Add more detail
- Create follow-up scenarios

**See:** [Contributing Guide](../CONTRIBUTING.md) for PR process

### Improve Documentation

Docs are never "done." Help make them better:

- **Fix typos** - Even small things help
- **Clarify confusing sections** - You understand now, help others
- **Add examples** - Show how to do something common
- **Improve FAQ** - What questions do you have?
- **Translate** - Help non-English speakers (future goal)

### Help Others

The best way to build community is to help:

- **Answer Q&A discussions** - If you know the answer, share it
- **Review PRs** - Give feedback on pull requests
- **Test releases** - Try new versions, report issues
- **Mentor new contributors** - Help them succeed

---

## Code of Conduct

We're committed to providing a welcoming, inclusive community.

**Our pledge:**
- Be respectful to all
- Welcome diverse perspectives
- Focus on what's best for the community
- Hold ourselves to high standards

**Unacceptable behavior:**
- Harassment, discrimination, hateful comments
- Threats, violence
- Unwanted sexual advances
- Doxxing, publishing private info
- Spam, trolling, bad faith arguments

**If you experience harassment:**
1. Report to maintainers (see [SECURITY.md](../SECURITY.md) for contact)
2. Include context and evidence if possible
3. Expect response within 48 hours
4. Confidentiality maintained throughout

**Full details:** [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)

---

## Communication Channels

### GitHub Issues
**For:** Bug reports, feature requests, work tracking
**Use when:** Something is broken, or you want a new feature

### GitHub Discussions
**For:** Questions, ideas, feedback, show and tell
**Use when:** Asking "how do I...", discussing approaches, sharing projects

### GitHub Security Advisory
**For:** Reporting security vulnerabilities
**See:** [SECURITY.md](../SECURITY.md)

### Email
**For:** Sensitive discussions with maintainers
**See:** [CONTRIBUTING.md](../CONTRIBUTING.md) for contact

---

## Moderating Discussions

**For maintainers:**

- Lock discussions that become off-topic
- Mark answers in Q&A discussions
- Pin announcements to the top
- Close discussions that turn into issues (move to Issues)
- Be respectful but firm on code of conduct

---

## Metrics to Track

Monitor community health:

- **Discussion activity** - New discussions, responses per week
- **Issue resolution time** - How fast do we resolve issues?
- **First response time** - How quickly do we respond?
- **Contributor diversity** - How many unique contributors?
- **Good first issue completion** - Do people complete them?

**Goal:** Foster a thriving, inclusive community where people feel welcome and heard.

---

## More Resources

- **[CONTRIBUTING.md](../CONTRIBUTING.md)** - How to contribute code
- **[CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)** - Community standards
- **[SECURITY.md](../SECURITY.md)** - Reporting security issues
- **[ROADMAP.md](../ROADMAP.md)** - What we're building next
- **[FAQ.md](FAQ.md)** - Common questions
- **[GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues)** - Join the conversation!
