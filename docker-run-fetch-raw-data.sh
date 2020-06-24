#!/bin/bash

set -e

PROJECT_NAME="$(<configuration/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-fetch-raw-data

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 4 ]]; then
    echo "Usage: ./docker-run-fetch-raw-data.sh
    [--profile-cpu <profile-output-path>]
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path>
    <raw-data-dir>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_GOOGLE_CLOUD_CREDENTIALS=$2
INPUT_PIPELINE_CONFIGURATION=$3
OUTPUT_RAW_DATA_DIR=$4

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="-m pyinstrument -o /data/cpu.prof --renderer html --"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
CMD="pipenv run python -u $PROFILE_CPU_CMD fetch_raw_data.py \
    \"$USER\" /credentials/google-cloud-credentials.json \
    /data/pipeline-configuration.json /data/Raw\ Data
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $INPUT_GOOGLE_CLOUD_CREDENTIALS -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"

echo "Copying $INPUT_PIPELINE_CONFIGURATION -> $container_short_id:/data/pipeline-configuration.json"
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline-configuration.json"

mkdir -p "$OUTPUT_RAW_DATA_DIR"
echo "Copying $OUTPUT_RAW_DATA_DIR/. -> $container_short_id:/data/Raw Data/"
docker cp "$OUTPUT_RAW_DATA_DIR/." "$container:/data/Raw Data/"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/Raw Data/. -> $OUTPUT_RAW_DATA_DIR"
docker cp "$container:/data/Raw Data/." "$OUTPUT_RAW_DATA_DIR"

if [[ "$PROFILE_CPU" = true ]]; then
    echo "Copying $container_short_id:/data/cpu.prof -> $CPU_PROFILE_OUTPUT_PATH"
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
