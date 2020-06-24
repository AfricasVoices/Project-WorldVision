FROM python:3.6-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Install memory_profiler if this script is run with PROFILE_MEMORY flag
ARG INSTALL_MEMORY_PROFILER="false"
RUN if [ "$INSTALL_MEMORY_PROFILER" = "true" ]; then \
        apt-get update && apt-get install -y gcc && \
        pip install memory_profiler; \
    fi

# Install plotly depedencies (orca and gcc)
ARG ORCA_VERSION="1.2.1"
RUN apt-get update && \
    apt-get install -y \
        wget \
        xvfb \
        xauth \
        libgtk2.0-0 \
        libxtst6 \
        libxss1 \
        libgconf-2-4 \
        libnss3 \
        libasound2 \
        gcc && \
    mkdir -p /opt/orca && \
    cd /opt/orca && \
    wget --no-verbose -O /opt/orca/orca-${ORCA_VERSION}.AppImage https://github.com/plotly/orca/releases/download/v${ORCA_VERSION}/orca-${ORCA_VERSION}-x86_64.AppImage && \
    chmod +x orca-${ORCA_VERSION}.AppImage && \
    ./orca-${ORCA_VERSION}.AppImage --appimage-extract && \
    rm orca-${ORCA_VERSION}.AppImage && \
    printf '#!/bin/bash \nxvfb-run --auto-servernum --server-args "-screen 0 640x480x24" /opt/orca/squashfs-root/app/orca "$@"' > /usr/bin/orca && \
    chmod +x /usr/bin/orca

# Make a directory for private credentials files
RUN mkdir /credentials

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD code_schemes/*.json /app/code_schemes/
ADD geojson/* /app/geojson/
ADD configuration/*.py /app/configuration/
ADD src /app/src
ADD fetch_raw_data.py /app
ADD generate_outputs.py /app
ADD upload_analysis_files.py /app
ADD automated_analysis.py /app
