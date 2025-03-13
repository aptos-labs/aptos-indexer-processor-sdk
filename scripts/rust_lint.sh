#!/bin/sh

# This script runs linting for both the examples and aptos-indexer-processors-sdk directories
# It assumes you have cargo-sort installed. If not, you can install it with:
# cargo install cargo-sort

set -e

# Function to run linting in a directory
run_lint() {
    local dir=$1
    echo "Running lint in $dir..."
    
    # Change to the directory
    cd "$dir"
    
    # Run in check mode if requested
    CHECK_ARG=""
    if [ "$1" = "--check" ]; then
        CHECK_ARG="--check"
    fi
    
    # Run the linting commands
    cargo +nightly xclippy
    cargo +nightly fmt $CHECK_ARG
    cargo sort --grouped --workspace $CHECK_ARG
    
    # Return to the original directory
    cd ..
}

# Make sure we're in the root directory
if [ ! -d "examples" ] || [ ! -d "aptos-indexer-processors-sdk" ]; then
    echo "Please run this script from the root directory of the project"
    exit 1
fi

# Run linting for both directories
echo "Starting linting process..."

echo "\nLinting examples directory..."
run_lint "examples"

echo "\nLinting aptos-indexer-processors-sdk directory..."
run_lint "aptos-indexer-processors-sdk"

echo "\nLinting completed successfully!" 