#!/usr/bin/env bash

set -e

if [ $# -ne 1 ]; then
    echo "Usage: ./checkout_coda_v2.sh <coda-v2-dir>"
    echo "Ensures that a copy of the CodaV2 project exists in 'coda-v2-dir' by cloning/fetching as necessary"
    exit
fi

CODA_V2_DIRECTORY="$1"

CODA_V2_REPO="https://github.com/AfricasVoices/CodaV2.git"

mkdir -p "$CODA_V2_DIRECTORY"
cd "$CODA_V2_DIRECTORY"

# If the CODA_V2_DIR does not contain a git repository, clone the coda v2 repo
if ! [ -d .git ]; then
    git clone "$CODA_V2_REPO" .
    cd data_tools
    pipenv --three && pipenv sync
fi

# Check that this repository is connected to the correct remote.
# (this ensures we are in the correct repository and can fetch new changes later if we need to)
if [ $(git config --get remote.origin.url) != "$CODA_V2_REPO" ]; then
    echo "Error: Git repository in <coda-v2-dir> does not have its origin set to $CODA_V2_REPO"
    exit 1
fi
