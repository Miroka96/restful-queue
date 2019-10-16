#!/bin/bash

docker run \
-p 127.0.0.1:3306:3306 \
--name mysql \
-e MYSQL_DATABASE=queue \
-e MYSQL_USER=queue \
-e MYSQL_PASSWORD=secret \
-e MYSQL_RANDOM_ROOT_PASSWORD=yes \
-d \
mysql