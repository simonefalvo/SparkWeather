#!/bin/bash

docker-compose exec broker /data/create_connect_topics.sh
docker-compose exec connect /data/load_testnifi_connectors.sh
