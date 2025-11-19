#!/bin/bash

bash docker/ysql_server/compose.sh

bash docker/http_bridge/compose.sh
