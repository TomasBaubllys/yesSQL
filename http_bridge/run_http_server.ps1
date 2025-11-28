# run.ps1

# Exit immediately if any command fails
$ErrorActionPreference = "Stop"

# Navigate to the project root (optional, adjust path if needed)
Set-Location $PSScriptRoot

# Step 1: Run the Python script
Write-Host "Running extract_command_codes.py..." -ForegroundColor Green
python app/extract_command_codes.py

# Step 2: Start Uvicorn
Write-Host "Starting Uvicorn server..." -ForegroundColor Green
uvicorn app.main:app --reload