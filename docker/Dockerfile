# Builder stage
FROM debian:bullseye-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends git cmake build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source code first
COPY lsm_tree/ /app/lsm_tree
COPY server/ /app/server

# Compile
RUN g++ -std=c++20 -pthread -O2 -o server_bin server/src/server.cpp server/src/main.cpp server/src/partition_server.cpp server/src/primary_server.cpp

# Final stage
FROM debian:bullseye-slim

WORKDIR /app

# Copy only the compiled binary
COPY --from=builder /app/server_bin .

ENTRYPOINT ["./server_bin"]
