#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            CPU_PROFILE_OUTPUT_PATH="$2"

            CPU_PROFILE_ARG="--profile-cpu $CPU_PROFILE_OUTPUT_PATH"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 4 ]]; then
    echo "Usage: ./2_fetch_raw_data.sh [--profile-cpu <cpu-profile-output-path>] <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <data-root>"
    echo "Fetches all the raw data from Rapid Pro and converts to TracedData"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
DATA_ROOT=$4

mkdir -p "$DATA_ROOT/Raw Data"

cd ..
./docker-run-fetch-raw-data.sh ${CPU_PROFILE_ARG} \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" "$DATA_ROOT/Raw Data"
