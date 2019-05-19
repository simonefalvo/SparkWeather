#!/bin/bash

kafka-topics --create --topic connect-config --partitions 1 \
    --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics --create --topic connect-status --partitions 1 \
    --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics --create --topic connect-offsets --partitions 1 \
    --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics --create --topic connect-data --partitions 1 \
    --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
