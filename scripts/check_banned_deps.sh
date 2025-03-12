#!/bin/sh

# This script checks if the crates in both examples and aptos-indexer-processors-sdk
# depend on external deps that they shouldn't. We run this in CI to make sure we don't
# accidentally reintroduce deps that would make the crates unusable for the CLI.
#
# While it would be more reliable to actually build the crate and check what libraries
# it links to, e.g. with otool, it is much cheaper to use cargo tree. As far as I can
# tell the entire Rust ecosystem makes use of these `x-sys` libraries to depend on
# external dynamically linked libraries.
#
# Run this from the root directory of the project.

# Make sure we're in the root directory
if [ ! -d "examples" ] || [ ! -d "aptos-indexer-processors-sdk" ]; then
    echo "Please run this script from the root directory of the project"
    exit 1
fi

# We only run the check on the SDK since that's the only crate used by the CLI. 
cd "aptos-indexer-processors-sdk"

declare -a deps=("pq-sys" "openssl-sys")

for dep in "${deps[@]}"; do
    echo "Checking for banned dependency $dep..."

    # Check for deps. As you can see, we only check for MacOS right now.
    # We specify --features postgres_partial because we only care about these banned deps
    # for the local testnet use case, in which case it uses only a subset of the
    # postgres features that don't include pq-sys. 
    out=`cargo tree --features postgres_partial -e features,no-build,no-dev --target aarch64-apple-darwin -i "$dep"`

    # If the exit status was non-zero, great, the dep couldn't be found.
    if [ $? -ne 0 ]; then
        continue
    fi

    # If the exit status was zero we have to check the output to see if the dep is in
    # use. If it is in the output, it is in use.
    if [[ $out != *"$dep"* ]]; then
        continue
    fi

    echo "Banned dependency $dep found in $dir!"
    cd ../..
    exit 1
done

echo "None of the banned dependencies are in use in $dir, great!"

exit 0