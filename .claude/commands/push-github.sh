#!/bin/bash
# Automated pipeline: Push Context Engine release to GitHub with tag
set -e

cd /data/git-root/ContextEngine

# Check github remote exists
if ! git remote get-url github >/dev/null 2>&1; then
    echo "ERROR: No github remote configured"
    exit 1
fi

# Detect latest tarball
TARBALL=$(ls -t context-engine-runtime-*.tar 2>/dev/null | head -1)
if [ -z "$TARBALL" ]; then
    echo "ERROR: No tarball found for release"
    exit 1
fi

# Extract version from tarball filename
VERSION=$(echo "$TARBALL" | sed 's/context-engine-runtime-v\([^-]*\).*/\1/')
SIZE=$(ls -lh "$TARBALL" | awk '{print $5}')

# Check if release already exists on github (check actual release, not just tag)
RELEASE_EXISTS=false
if command -v gh >/dev/null 2>&1; then
    if gh release view "v${VERSION}" --repo Pewejekubam/ContextEngine >/dev/null 2>&1; then
        RELEASE_EXISTS=true
        echo "Release v${VERSION} already exists on GitHub"
        exit 0
    fi
elif git ls-remote github "refs/tags/v${VERSION}" | grep -q .; then
    echo "Tag v${VERSION} exists (cannot verify release without gh CLI)"
fi

# Delete local tag if exists
git tag -d "v${VERSION}" 2>/dev/null || true

# Check for uncommitted changes
HAS_CHANGES=false
if git status --short | grep -q .; then
    HAS_CHANGES=true

    # Stage all changes
    git add .

    # Force-add tarball (gitignored)
    git add -f "$TARBALL"

    # Create commit
    git commit -m "Release v${VERSION}

Distribution release
- Tarball: ${TARBALL}
- Size: ${SIZE}

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
fi

# Create version tag (skip if already exists)
if ! git tag "v${VERSION}" 2>/dev/null; then
    echo "Tag v${VERSION} already exists locally"
fi

# Push to GitHub (code changes only, tags separately)
if ! git push github main; then
    # Try rebase if push rejected
    git fetch github
    git rebase github/main
    git push github main
fi

# Push tag (force update if exists)
if ! git push github "v${VERSION}" 2>/dev/null; then
    echo "Tag v${VERSION} already exists on remote, force updating..."
    git push github :refs/tags/v${VERSION} 2>/dev/null || true
    git push github "v${VERSION}"
fi

# Create GitHub release
if command -v gh >/dev/null 2>&1; then
    # Use gh CLI if available
    gh release delete "v${VERSION}" -y 2>/dev/null || true
    gh release create "v${VERSION}" "$TARBALL" \
        --title "Context Engine v${VERSION}" \
        --notes "Distribution release v${VERSION}

Download and extract the tarball to use Context Engine.

See README.md for installation instructions."
else
    # Fallback to GitHub API with curl
    if [ -f .env ]; then source .env; fi
    if [ -n "$GITHUB_TOKEN" ]; then
        RELEASE_NOTES="Distribution release v${VERSION}

Download and extract the tarball to use Context Engine.

See README.md for installation instructions."

        # Delete existing release if it exists
        EXISTING_RELEASE_ID=$(curl -s \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "Accept: application/vnd.github+json" \
            https://api.github.com/repos/Pewejekubam/ContextEngine/releases/tags/v${VERSION} \
            | jq -r '.id // empty')

        if [ -n "$EXISTING_RELEASE_ID" ] && [ "$EXISTING_RELEASE_ID" != "null" ]; then
            echo "Deleting existing release v${VERSION}..."
            curl -s -X DELETE \
                -H "Authorization: Bearer ${GITHUB_TOKEN}" \
                -H "Accept: application/vnd.github+json" \
                https://api.github.com/repos/Pewejekubam/ContextEngine/releases/${EXISTING_RELEASE_ID}
        fi

        # Create release
        RELEASE_ID=$(curl -s -X POST \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "Accept: application/vnd.github+json" \
            https://api.github.com/repos/Pewejekubam/ContextEngine/releases \
            -d "{\"tag_name\":\"v${VERSION}\",\"name\":\"Context Engine v${VERSION}\",\"body\":$(echo "$RELEASE_NOTES" | jq -Rs .)}" \
            | jq -r '.id')

        if [ "$RELEASE_ID" != "null" ] && [ -n "$RELEASE_ID" ]; then
            # Upload tarball asset
            curl -s -X POST \
                -H "Authorization: Bearer ${GITHUB_TOKEN}" \
                -H "Content-Type: application/x-tar" \
                --data-binary "@${TARBALL}" \
                "https://uploads.github.com/repos/Pewejekubam/ContextEngine/releases/${RELEASE_ID}/assets?name=${TARBALL}" >/dev/null
            echo "GitHub release created with API"
        else
            echo "WARNING: GitHub release not created (API error)"
        fi
    else
        echo "WARNING: GitHub release not created (no gh CLI and no GITHUB_TOKEN)"
    fi
fi

# Report completion
COMMIT=$(git rev-parse --short HEAD)
echo ""
echo "✓ Released to GitHub"
echo "Version: v${VERSION}"
echo "Tag: v${VERSION}"
echo "Tarball: ${TARBALL} (${SIZE})"
echo "Commit: ${COMMIT}"

# Cleanup old tarballs (keep latest 2)
ls -t context-engine-runtime-*.tar | tail -n +3 | xargs rm -f 2>/dev/null || true
