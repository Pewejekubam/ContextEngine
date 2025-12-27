# Contributing to Context Engine

Thank you for your interest in contributing to Context Engine!

## How to Contribute

### Reporting Issues

- Use GitHub Issues to report bugs or request features
- Include your Python version, OS, and SQLite version
- Provide steps to reproduce the issue
- Include relevant error messages or logs

### Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Test your changes thoroughly
5. Commit with clear, descriptive messages
6. Push to your fork
7. Open a Pull Request

### Code Style

- Python code should follow PEP 8
- Use clear, descriptive variable and function names
- Add docstrings to functions
- Keep functions focused and small

### Testing

Before submitting:
- Run `make tags-check` to verify health checks pass
- Test with a fresh database (`make database-clean && make database-init`)
- Verify chatlog extraction works (`make chatlogs-extract`)

### Documentation

- Update README.md if adding features
- Document new configuration options
- Add comments for complex logic

## Questions & Discussions

- **Technical questions?** - Open an [Issue](https://github.com/Pewejekubam/ContextEngine/issues) with the `question` label
- **Want to discuss an idea first?** - [Open an issue](https://github.com/Pewejekubam/ContextEngine/issues/new?labels=enhancement&title=Proposal:) before submitting PR
- **Need help getting started?** - See [Community Guide](docs/COMMUNITY.md) and [FAQ](docs/FAQ.md)

## Good First Issues

Looking to contribute but not sure where to start?

Check issues labeled **`💚 good first issue`** - these are specifically chosen for new contributors and include:
- Clear problem statement
- Limited scope
- Implementation guidance
- Estimation of effort

See [Community Guide](docs/COMMUNITY.md#good-first-issue) for how we choose and label these.

## Report Security Issues

**Do NOT open a public issue for security vulnerabilities.**

See [SECURITY.md](SECURITY.md) for responsible disclosure process.

## Community

- **[GitHub Issues](https://github.com/Pewejekubam/ContextEngine/issues)** - Q&A, ideas, feedback
- **[FAQ](docs/FAQ.md)** - Common questions
- **[Community Guide](docs/COMMUNITY.md)** - How to engage
- **[ROADMAP](ROADMAP.md)** - Where we're headed
- **[Code of Conduct](CODE_OF_CONDUCT.md)** - Community standards

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
