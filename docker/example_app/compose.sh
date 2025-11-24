#!/bin/bash

current_dir=$PWD

expected_dir="${HOME}/Documents/yesSQL"

if [[ "$current_dir" != "$expected_dir" ]]; then
	echo "Script must be ran from yesSQL as the root folder"
   echo	"Go to yesSQL folder and run \"bash docker/compose.sh\""
	exit
fi

echo "Building docker image"
docker build -t example_app -f docker/example_app/Dockerfile .
echo "done"

echo "Building docker containers"
docker run -d --name example_app --rm -it -p 8080:8080 example_app
echo "Done"
