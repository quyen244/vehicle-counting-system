#!/bin/bash

# 1. Khởi động SSH Server
echo "Starting SSH Server..."
/usr/sbin/sshd

# 2. Kiểm tra chế độ chạy (Master hay Worker)
if [ "$SPARK_MODE" == "master" ]; then
    echo "Starting Spark Master..."
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --ip spark-master
        
elif [ "$SPARK_MODE" == "worker" ]; then
    echo "Starting Spark Worker connecting to $SPARK_MASTER_URL..."
    # Sử dụng biến SPARK_WORKER_MEMORY từ docker-compose, mặc định 1G nếu không có
    MEM=${SPARK_WORKER_MEMORY:-1G}
    CORES=${SPARK_WORKER_CORES:-1}
    
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        $SPARK_MASTER_URL \
        --memory $MEM \
        --cores $CORES
else
    echo "Error: SPARK_MODE not set or invalid (current: $SPARK_MODE)"
    echo "Please set SPARK_MODE to 'master' or 'worker' in docker-compose.yml"
    exit 1
fi