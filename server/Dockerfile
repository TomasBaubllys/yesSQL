# Builder stage
FROM debian:bullseye-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends git cmake build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source code first
COPY . .

# Compile
RUN g++ -std=c++20 -O2 -o server src/server.cpp src/main.cpp

# Final stage
FROM debian:bullseye-slim

WORKDIR /app

# Copy only the compiled binary
COPY --from=builder /app/server .

EXPOSE 8080

CMD ["./server"]
