#!/bin/bash

# Ensure Docker BuildKit is used
export DOCKER_BUILDKIT=1

# Build the image
docker build -t server .

# Stop and remove any existing container named "server"
docker rm -f server 2>/dev/null || true

# Run the server container in detached mode, exposing port 8080
docker run -d -p 8080:8080 --name server server
