#!/bin/bash
# Script to clean macOS metadata files that cause Docker build issues on external drives

echo "Cleaning macOS metadata files..."

# Remove all ._* files (except in .git where we don't have permissions)
find . -name "._*" -type f ! -path "./.git/*" -delete 2>/dev/null

# Also try dot_clean if available
if command -v dot_clean &> /dev/null; then
    echo "Using dot_clean..."
    dot_clean . 2>/dev/null
fi

echo "Done! Try running 'make up' again."
