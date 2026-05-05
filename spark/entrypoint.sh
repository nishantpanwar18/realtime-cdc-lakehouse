#!/bin/bash

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi
