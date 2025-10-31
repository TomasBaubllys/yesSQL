# Builder stage
FROM debian:bookworm-slim AS builder

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
RUN cd lsm_tree && make clean && make
RUN cd server && make clean && make 

# Final stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy only the compiled binary
COPY --from=builder /app/server/bin/server ./server

ENTRYPOINT ["./server"]
