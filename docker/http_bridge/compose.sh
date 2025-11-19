#!/bin/bash

current_dir=$PWD

expected_dir="${HOME}/Documents/yesSQL"

if [[ "$current_dir" != "$expected_dir" ]]; then
	echo "Script must be ran from yesSQL as the root folder"
   echo	"Go to yesSQL folder and run \"bash docker/http_bridge/compose.sh\""
	exit
fi

# docker build -t main_server -f docker/main_server/Dockerfile .

echo "Building docker image"
docker build -t http_bridge -f docker/http_bridge/Dockerfile .
echo "Done"

echo "Building docker container"
docker run -d --name http_bridge  --rm -it -p 8000:8000 http_bridge
echo "Done"
