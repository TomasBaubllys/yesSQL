# --------------------------------------------
# Load environment variables
# --------------------------------------------
if (Test-Path "./docker/database_setup.env.ps1") {
    . ./docker/ysql_server/database_setup.env.ps1
} else {
    Write-Host "Environment file docker/database_setup.env.ps1 not found."
    exit
}

# --------------------------------------------
# Options
# --------------------------------------------
$BUILD_FRESH_OPTION = "--fresh"
$REMOVE_IMAGES_OPTION = "--rimages"

# --------------------------------------------
# Set the expected directory (YOUR ACTUAL PATH)
# --------------------------------------------
$expected_dir = "C:\Users\Ugne\Desktop\NoSQL\yesSQL"
$current_dir = (Get-Location).Path.TrimEnd('\')

if ($current_dir -ne $expected_dir) {
    Write-Host "Script must be run from yesSQL as the root folder"
    Write-Host "Go to yesSQL folder and run: powershell -File docker/windows.ps1"
    exit
}

# --------------------------------------------
# Handle --fresh option
# --------------------------------------------
if ($args.Count -ge 1 -and $args[0] -eq $BUILD_FRESH_OPTION) {
    Write-Host "Decomposing docker containers..."
    docker compose --file docker/ysql_server/docker-compose.yml down
    Write-Host "Done"
}

# --------------------------------------------
# Handle --rimages option
# --------------------------------------------
if ($args.Count -ge 2 -and $args[1] -eq $REMOVE_IMAGES_OPTION) {
    Write-Host "Deleting old images..."
    docker rmi server --force
    Write-Host "Done"
}

# --------------------------------------------
# Build Docker image
# --------------------------------------------
Write-Host "Building server image..."
docker build -t server -f docker/ysql_server/Dockerfile .
Write-Host "Done"

# --------------------------------------------
# Start containers
# --------------------------------------------
if (!$env:PARTITION_COUNT) {
    Write-Host "PARTITION_COUNT is not set. Define it in docker/database_setup.env.ps1"
    exit
}

Write-Host "Starting docker containers..."
docker compose --file docker/ysql_server/docker-compose.yml up --scale partition_server=$env:PARTITION_COUNT -d
Write-Host "Done"
