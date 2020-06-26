#!/bin/bash

set -e

PROJECT_NAME="$(<configuration/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-generate-outputs

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
         --profile-memory)
            PROFILE_MEMORY=true
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done


# Check that the correct number of arguments were provided.
if [[ $# -ne 11 ]]; then
    echo "Usage: ./docker-run-generate-outputs.sh
    [--profile-cpu <profile-output-path>] [--profile-memory <profile-output-path>]
    <user> <pipeline-configuration-file-path>
    <raw-data-dir> <prev-coded-dir> <messages-json-output-path> <individuals-json-output-path>
    <icr-output-dir> <coded-output-dir> <messages-output-csv> <individuals-output-csv> <production-output-csv>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_PIPELINE_CONFIGURATION=$2
INPUT_RAW_DATA_DIR=$3
PREV_CODED_DIR=$4
OUTPUT_MESSAGES_JSONL=$5
OUTPUT_INDIVIDUALS_JSONL=$6
OUTPUT_ICR_DIR=$7
OUTPUT_CODED_DIR=$8
OUTPUT_MESSAGES_CSV=$9
OUTPUT_INDIVIDUALS_CSV=${10}
OUTPUT_PRODUCTION_CSV=${11}

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_MEMORY_PROFILER="$PROFILE_MEMORY" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="-m pyinstrument -o /data/cpu.prof --renderer html --"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
if [[ "$PROFILE_MEMORY" = true ]]; then
    PROFILE_MEMORY_CMD="mprof run -o /data/memory.prof"
fi
CMD="pipenv run $PROFILE_MEMORY_CMD python -u $PROFILE_CPU_CMD generate_outputs.py \
    \"$USER\" /data/pipeline_configuration.json /data/raw-data /data/prev-coded \
    /data/output-messages.jsonl /data/output-individuals.jsonl /data/output-icr /data/coded \
    /data/output-messages.csv /data/output-individuals.csv /data/output-production.csv \
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $INPUT_PIPELINE_CONFIGURATION -> $container_short_id:/data/pipeline_configuration.json"
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"

echo "Copying $INPUT_RAW_DATA_DIR -> $container_short_id:/data/raw-data"
docker cp "$INPUT_RAW_DATA_DIR" "$container:/data/raw-data"

if [[ -d "$PREV_CODED_DIR" ]]; then
    echo "Copying $PREV_CODED_DIR -> $container_short_id:/data/prev-coded"
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
else
    echo "WARNING: prev-coded-dir $PREV_CODED_DIR not found, ignoring"  # TODO: Stop allowing this to be optional.
fi

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/output-messages.jsonl -> $OUTPUT_MESSAGES_JSONL"
mkdir -p "$(dirname "$OUTPUT_MESSAGES_JSONL")"
docker cp "$container:/data/output-messages.jsonl" "$OUTPUT_MESSAGES_JSONL"

echo "Copying $container_short_id:/data/output-individuals.jsonl -> $OUTPUT_INDIVIDUALS_JSONL"
mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_JSONL")"
docker cp "$container:/data/output-individuals.jsonl" "$OUTPUT_INDIVIDUALS_JSONL"

echo "Copying $container_short_id:/data/output-icr/. -> $OUTPUT_ICR_DIR"
mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

echo "Copying $container_short_id:/data/coded/. -> $OUTPUT_CODED_DIR"
mkdir -p "$OUTPUT_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_CODED_DIR"

echo "Copying $container_short_id:/data/output-production.csv -> $OUTPUT_PRODUCTION_CSV"
mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"

echo "Copying $container_short_id:/data/output-messages.csv -> $OUTPUT_MESSAGES_CSV"
mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
docker cp "$container:/data/output-messages.csv" "$OUTPUT_MESSAGES_CSV"

echo "Copying $container_short_id:/data/output-individuals.csv -> $OUTPUT_INDIVIDUALS_CSV"
mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

if [[ "$PROFILE_CPU" = true ]]; then
    echo "Copying $container_short_id:/data/cpu.prof -> $CPU_PROFILE_OUTPUT_PATH"
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

if [[ "$PROFILE_MEMORY" = true ]]; then
    echo "Copying $container_short_id:/data/memory.prof -> $MEMORY_PROFILE_OUTPUT_PATH"
    mkdir -p "$(dirname "$MEMORY_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/memory.prof" "$MEMORY_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
