#!/usr/bin/env bash

set -e

if [[ $# -ne 5 ]]; then
    echo "Usage: ./8_upload_log_files.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <memory-profile-dir> <data-archive-dir>"
    echo "Uploads the pipeline's analysis files"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
MEMORY_PROFILE_DIR=$4
DATA_ARCHIVE_DIR=$5

cd ..
pipenv run python upload_log_files.py "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$MEMORY_PROFILE_DIR" "$DATA_ARCHIVE_DIR"
