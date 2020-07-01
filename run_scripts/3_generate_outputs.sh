#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            CPU_PROFILE_OUTPUT_PATH="$2"
            CPU_PROFILE_ARG="--profile-cpu $CPU_PROFILE_OUTPUT_PATH"
            shift 2;;
        --profile-memory)
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            MEMORY_PROFILE_ARG="--profile-memory $MEMORY_PROFILE_OUTPUT_PATH"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 3 ]]; then
    echo "Usage: ./3_generate_outputs.sh [--profile-cpu <cpu-profile-output-path>] [--profile-memory <memory-profile-output-path>] <user> <pipeline-configuration-file-path> <data-root>"
    echo "Generates ICR files, Coda files, production CSV and analysis CSVs from the raw data files produced by run scripts 1 and 2"
    exit
fi

USER=$1
PIPELINE_CONFIGURATION_FILE_PATH=$2
DATA_ROOT=$3

mkdir -p "$DATA_ROOT/Coded Coda Files"
mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run-generate-outputs.sh ${CPU_PROFILE_ARG} ${MEMORY_PROFILE_ARG} \
    "$USER" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$DATA_ROOT/Raw Data" "$DATA_ROOT/Coded Coda Files/" \
    "$DATA_ROOT/Outputs/messages_traced_data.jsonl" "$DATA_ROOT/Outputs/individuals_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/ICR/" "$DATA_ROOT/Outputs/Coda Files/" \
    "$DATA_ROOT/Outputs/messages.csv" "$DATA_ROOT/Outputs/individuals.csv" \
    "$DATA_ROOT/Outputs/production.csv"
