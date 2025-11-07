#!/bin/bash

source ./docker/database_setup.env
echo $PARTITION_COUNT

# composes down current containers
BUILD_FRESH_OPTION="--fresh"

# rebuilds the images, can only be ran with --fresh
REMOVE_IMAGES_OPTION="--rimages"

current_dir=$PWD

expected_dir="${HOME}/Documents/yesSQL"

if [[ "$current_dir" != "$expected_dir" ]]; then
	echo "Script must be ran from yesSQL as the root folder"
   echo	"Go to yesSQL folder and run \"bash docker/compose.sh\""
	exit
fi

if [[ "$1" == "$BUILD_FRESH_OPTION" ]]; then
	echo "Decomposing docker containers..."
	docker compose --file docker/docker-compose.yml down
	echo "Done"
fi

if [[ "$2" == "$REMOVE_IMAGES_OPTION" ]]; then
	echo "Deleting old images"
	docker rmi server --force
	echo "Done"
fi

# docker build -t main_server -f docker/main_server/Dockerfile .

docker build -t server -f docker/Dockerfile .

echo "Building docker containers"
docker compose --file docker/docker-compose.yml up --scale partition_server=${PARTITION_COUNT} -d
echo "Done"
