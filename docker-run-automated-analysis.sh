#!/bin/bash

set -e

PROJECT_NAME="$(<configuration/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-automated-analysis

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
if [[ $# -ne 6 ]]; then
    echo "Usage: ./docker-run.sh
    [--profile-cpu <profile-output-path>]
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path>
    <messages-traced-data> <individuals-traced-data> <automated-analysis-output-dir>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_GOOGLE_CLOUD_CREDENTIALS=$2
INPUT_PIPELINE_CONFIGURATION=$3
INPUT_MESSAGES_TRACED_DATA=$4
INPUT_INDIVIDUALS_TRACED_DATA=$5
AUTOMATED_ANALYSIS_OUTPUT_DIR=$6

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="-m pyinstrument -o /data/cpu.prof --renderer html --"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
CMD="pipenv run python -u $PROFILE_CPU_CMD automated_analysis.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json \
    /data/messages-traced-data.jsonl /data/individuals-traced-data.jsonl /data/automated-analysis-outputs
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $INPUT_PIPELINE_CONFIGURATION -> $container_short_id:/data/pipeline_configuration.json"
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"

echo "Copying $INPUT_GOOGLE_CLOUD_CREDENTIALS -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"

echo "Copying $INPUT_MESSAGES_TRACED_DATA -> $container_short_id:/data/messages-traced-data.jsonl"
docker cp "$INPUT_MESSAGES_TRACED_DATA" "$container:/data/messages-traced-data.jsonl"

echo "Copying $INPUT_INDIVIDUALS_TRACED_DATA -> $container_short_id:/data/individuals-traced-data.jsonl"
docker cp "$INPUT_INDIVIDUALS_TRACED_DATA" "$container:/data/individuals-traced-data.jsonl"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/automated-analysis-outputs/. -> $AUTOMATED_ANALYSIS_OUTPUT_DIR"
mkdir -p "$AUTOMATED_ANALYSIS_OUTPUT_DIR"
docker cp "$container:/data/automated-analysis-outputs/." "$AUTOMATED_ANALYSIS_OUTPUT_DIR"

if [[ "$PROFILE_CPU" = true ]]; then
    echo "Copying $container_short_id:/data/cpu.prof -> $CPU_PROFILE_OUTPUT_PATH"
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
