# compose.ps1

$current_dir = Get-Location

# docker build -t main_server -f docker/main_server/Dockerfile .

Write-Host "Building docker image" -ForegroundColor Green
docker build -t http_bridge -f docker/http_bridge/Dockerfile .
Write-Host "Done" -ForegroundColor Green

Write-Host "Building docker container" -ForegroundColor Green
docker run -d --name http_bridge --rm -it -p 8000:8000 http_bridge
Write-Host "Done" -ForegroundColor Green