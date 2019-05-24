#!/bin/bash
docker network create dist_default
docker run --rm -t -i -p 27017:27017 --network=dist_default --name mongo_server mongo /usr/bin/mongod --smallfiles --bind_ip_all
