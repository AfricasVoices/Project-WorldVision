#!/usr/bin/env bash

set -e

if [[ $# -ne 9 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <pipeline-configuration-json>"
    echo "  <coda-pull-credentials-path> <coda-push-credentials-path> <avf-bucket-credentials-path>"
    echo "  <coda-tools-root> <data-root> <data-backup-dir> <performance-logs-dir>"
    echo "Runs the pipeline end-to-end (data fetch, coda fetch, output generation, Drive upload, Coda upload, data backup)"
    exit
fi

USER=$1
PIPELINE_CONFIGURATION=$2
CODA_PULL_CREDENTIALS_PATH=$3
CODA_PUSH_CREDENTIALS_PATH=$4
AVF_BUCKET_CREDENTIALS_PATH=$5
CODA_TOOLS_ROOT=$6
DATA_ROOT=$7
DATA_BACKUPS_DIR=$8
PERFORMANCE_LOGS_DIR=$9

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"

echo "Starting run with id '$RUN_ID'"

./1_coda_get.sh "$CODA_PULL_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

./2_fetch_raw_data.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./3_generate_outputs.sh --profile-memory "$PERFORMANCE_LOGS_DIR/memory-$RUN_ID.profile" \
    "$USER" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./4_coda_add.sh "$CODA_PUSH_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

./5_automated_analysis.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./6_backup_data_root.sh "$DATA_ROOT" "$DATA_BACKUPS_DIR/data-$RUN_ID.tar.gzip"

./7_upload_analysis_files.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$RUN_ID" "$DATA_ROOT"

./8_upload_log_files.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$PERFORMANCE_LOGS_DIR" "$DATA_BACKUPS_DIR"
