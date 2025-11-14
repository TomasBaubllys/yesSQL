#!/bin/bash

# Exit immediately if any command fails
set -e

# Navigate to the project root (optional, adjust path if needed)
cd "$(dirname "$0")"

# Step 1: Run the Python script
echo "Running extract_command_codes.py..."
python3 app/extract_command_codes.py

# Step 2: Start Uvicorn
echo "Starting Uvicorn server..."
uvicorn app.main:app --reload
