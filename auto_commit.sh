#!/bin/bash

# Exit on error
set -e

# Move to the script's directory (assumes script is in repo root)
cd "$(dirname "$0")"

# Ensure it's a Git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "❌ Error: This is not a Git repository."
    exit 1
fi

# Check for changes
if [[ -n $(git status --porcelain) ]]; then
    echo "✅ Changes detected."

    # Prompt for commit message
    read -rp "Enter commit message (leave blank to use timestamp): " user_msg

    # Use timestamp if no message provided
    if [[ -z "$user_msg" ]]; then
        user_msg="Auto-commit: $(date "+%Y-%m-%d %H:%M:%S")"
    fi

    # Stage and commit
    git add .
    git commit -m "$user_msg"

    # Push to main branch (uses stored credentials)
    echo "🚀 Pushing to main branch..."
    git push origin main

    echo "✅ Changes committed and pushed with message: \"$user_msg\""
else
    echo "ℹ️ No changes to commit."
fi

