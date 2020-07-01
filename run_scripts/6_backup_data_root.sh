#!/usr/bin/env bash

set -e

if [[ $# -ne 2 ]]; then
    echo "Usage: ./6_backup_data_root <data-root> <backup-location>"
    echo "Backs-up the data root directory to a compressed file in at the specified location"
    exit
fi

DATA_ROOT=$1
BACKUP_LOCATION=$2

mkdir -p "$(dirname "$BACKUP_LOCATION")"
find "$DATA_ROOT" -type f -name '.DS_Store' -delete
cd "$DATA_ROOT"
tar -czvf "$BACKUP_LOCATION" .
