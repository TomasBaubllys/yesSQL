$currentDir = Get-Location

Write-Host "Building docker image"
docker build -t example_app -f docker/example_app/Dockerfile .
Write-Host "done"

Write-Host "Building docker containers"
docker run --add-host=host.docker.internal:host-gateway --dns 8.8.8.8 --dns 1.1.1.1 -d --name example_app --rm -it -p 8080:8080 example_app
Write-Host "Done"