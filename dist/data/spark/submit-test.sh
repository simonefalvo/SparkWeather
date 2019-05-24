#!/bin/sh

/spark/bin/spark-submit --master spark://spark-master:7077 \
    /spark/examples/src/main/python/pi.py
