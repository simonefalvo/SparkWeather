#!/bin/bash
docker run --rm -t -i --network=dist_default --name=mongo_client mongo:latest /bin/bash
