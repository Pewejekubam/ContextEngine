#!/bin/bash
# Automated pipeline: Push Context Engine release to Gitea origin
set -e

cd /data/git-root/ContextEngine

# Load environment (for GITEA_TOKEN)
if [ -f .env ]; then
    source .env
fi

# Detect latest tarball
TARBALL=$(ls -t context-engine-runtime-*.tar 2>/dev/null | head -1)
if [ -z "$TARBALL" ]; then
    echo "ERROR: No tarball found"
    exit 1
fi

# Extract version from tarball filename
VERSION=$(echo "$TARBALL" | sed 's/context-engine-runtime-v\([^-]*\).*/\1/')
SIZE=$(ls -lh "$TARBALL" | awk '{print $5}')

# Track what we did
DID_COMMIT=false
DID_PUSH=false
DID_TAG=false

# Step 1: Check for uncommitted changes
if git status --short | grep -q .; then
    echo "Staging and committing changes..."

    # Stage all changes (respecting .gitignore)
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
    DID_COMMIT=true
fi

# Step 2: Check if we need to push
git fetch origin main --quiet 2>/dev/null || true
LOCAL_HEAD=$(git rev-parse HEAD)
REMOTE_HEAD=$(git rev-parse origin/main 2>/dev/null || echo "none")

if [ "$LOCAL_HEAD" != "$REMOTE_HEAD" ]; then
    echo "Pushing to origin..."
    if ! git push origin main; then
        # Try rebase if push rejected
        git pull --rebase origin main
        git push origin main
    fi
    DID_PUSH=true
fi

# Step 3: Check if tag exists
if ! git rev-parse "v${VERSION}" >/dev/null 2>&1; then
    echo "Creating tag v${VERSION}..."
    git tag -a "v${VERSION}" -m "Release v${VERSION}"
    DID_TAG=true
fi

# Step 4: Check if tag is pushed
if ! git ls-remote --tags origin "v${VERSION}" | grep -q "v${VERSION}"; then
    echo "Pushing tag v${VERSION}..."
    git push origin "v${VERSION}"
    DID_TAG=true
fi

# Report status
COMMIT=$(git rev-parse --short HEAD)
echo ""
if [ "$DID_COMMIT" = true ] || [ "$DID_PUSH" = true ] || [ "$DID_TAG" = true ]; then
    echo "✓ Synced to origin (Gitea)"
    [ "$DID_COMMIT" = true ] && echo "  - Created commit"
    [ "$DID_PUSH" = true ] && echo "  - Pushed to main"
    [ "$DID_TAG" = true ] && echo "  - Created/pushed tag v${VERSION}"
else
    echo "✓ Already in sync with origin"
fi
echo "Version: v${VERSION}"
echo "Tarball: ${TARBALL} (${SIZE})"
echo "Commit: ${COMMIT}"

# Create Gitea release
echo ""
echo "Checking Gitea release..."

if [ -z "${GITEA_TOKEN:-}" ]; then
    echo "⚠ GITEA_TOKEN not set, skipping release creation"
    echo "  Set GITEA_TOKEN in .env or run manually"
    exit 0
fi

# Create release via API
GITEA_URL="http://biz-srv58.corp.biztocloud.com:3001"
GITEA_OWNER="BizToCloud"
GITEA_REPO="ContextEngine"
API_URL="${GITEA_URL}/api/v1"

RELEASE_NOTES="Distribution release v${VERSION}

- Tarball: ${TARBALL}
- Size: ${SIZE}
- Commit: ${COMMIT}"

# Check if release already exists
EXISTING=$(curl -s -H "Authorization: token ${GITEA_TOKEN}" \
    "${API_URL}/repos/${GITEA_OWNER}/${GITEA_REPO}/releases/tags/v${VERSION}" | jq -r '.id // empty')

if [ -n "$EXISTING" ]; then
    echo "Release v${VERSION} already exists (ID: ${EXISTING}), deleting..."
    curl -s -X DELETE -H "Authorization: token ${GITEA_TOKEN}" \
        "${API_URL}/repos/${GITEA_OWNER}/${GITEA_REPO}/releases/${EXISTING}"
fi

# Create release
RELEASE_JSON=$(jq -n \
    --arg tag "v${VERSION}" \
    --arg name "Context Engine v${VERSION}" \
    --arg body "$RELEASE_NOTES" \
    '{tag_name: $tag, name: $name, body: $body, draft: false, prerelease: false}')

RESPONSE=$(curl -s -X POST \
    "${API_URL}/repos/${GITEA_OWNER}/${GITEA_REPO}/releases" \
    -H "Authorization: token ${GITEA_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$RELEASE_JSON")

RELEASE_ID=$(echo "$RESPONSE" | jq -r '.id')
if [ -z "$RELEASE_ID" ] || [ "$RELEASE_ID" == "null" ]; then
    echo "ERROR: Failed to create release"
    echo "$RESPONSE" | jq '.'
    exit 1
fi

echo "Created release (ID: ${RELEASE_ID})"

# Upload tarball
echo "Uploading tarball..."
UPLOAD_RESPONSE=$(curl -s -X POST \
    "${API_URL}/repos/${GITEA_OWNER}/${GITEA_REPO}/releases/${RELEASE_ID}/assets" \
    -H "Authorization: token ${GITEA_TOKEN}" \
    -F "attachment=@${TARBALL}")

ASSET_NAME=$(echo "$UPLOAD_RESPONSE" | jq -r '.name')
if [ -z "$ASSET_NAME" ] || [ "$ASSET_NAME" == "null" ]; then
    echo "ERROR: Failed to upload tarball"
    echo "$UPLOAD_RESPONSE" | jq '.'
    exit 1
fi

echo ""
echo "✓ Gitea release created"
echo "  Release: ${GITEA_URL}/${GITEA_OWNER}/${GITEA_REPO}/releases/tag/v${VERSION}"
echo "  Asset: ${ASSET_NAME}"
