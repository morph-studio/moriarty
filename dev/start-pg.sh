#!/usr/bin/env bash
docker stop postgresql
docker rm postgresql

docker run -d --name postgresql -p 5432:5432 \
-e POSTGRES_PASSWORD=Th3PAssW0rd!1. \
-e POSTGRES_USER=postgres \
-e POSTGRES_DB=moriarty-matrix \
postgres:14
