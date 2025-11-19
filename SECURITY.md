# Security Policy

## Supported Versions

We release patches for security vulnerabilities in the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 3.1.x   | :white_check_mark: |
| < 3.0   | :x:                |

## Reporting a Vulnerability

We take the security of Context Engine seriously. If you discover a security vulnerability, please report it responsibly.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via:
- **GitHub Security Advisories**: Use the "Security" tab in this repository to privately report a vulnerability
- **Email**: Contact the maintainers directly (see CONTRIBUTING.md for contact information)

### What to Include

When reporting a vulnerability, please include:
- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Initial Response**: Within 48 hours of receiving your report
- **Status Update**: Within 7 days with our assessment and planned timeline
- **Fix Release**: Depending on severity, typically within 30 days

### Security Update Process

1. We will confirm the vulnerability and determine its impact
2. We will develop and test a fix
3. We will release a security advisory and patched version
4. We will credit you in the security advisory (unless you prefer to remain anonymous)

## Security Best Practices

When using Context Engine:

1. **Keep Updated**: Always use the latest version to ensure you have security patches
2. **Protect Your Database**: The SQLite database (`data/rules.db`) may contain sensitive project information
   - Ensure proper file permissions (not world-readable)
   - Do not commit to version control
3. **Review Chatlogs**: Before sharing chatlogs, review for sensitive information (API keys, passwords, internal URLs)
4. **Access Control**: If deploying in shared environments, restrict access to the `.context-engine` directory
5. **Validate YAML**: Ensure chatlog YAML files are from trusted sources before extraction

## Known Security Considerations

### SQLite Database
- The `data/rules.db` file stores extracted knowledge
- By default, SQLite databases have no encryption
- For sensitive projects, consider encrypting the filesystem or using SQLite encryption extensions

### Chatlog Files
- YAML chatlogs may contain sensitive information from development sessions
- The `.gitignore` includes `data/chatlogs/*.yaml` by default
- Review before sharing externally

### Python Script Execution
- Context Engine scripts use Python with SQLite and YAML parsing
- Keep Python and PyYAML updated to receive security patches
- Scripts do not execute arbitrary code from chatlogs

## Disclosure Policy

We follow responsible disclosure:
- Security issues are fixed privately before public disclosure
- We coordinate with reporters on disclosure timing
- Public advisories are published after fixes are available
