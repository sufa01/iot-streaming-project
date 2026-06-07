#!/bin/bash
KAFKA_CONTAINER="kafka"   
BOOTSTRAP_SERVER="localhost:9092"
TOPICS=("iot-events" "iot-aggregates")
PARTITIONS=1
REPLICATION_FACTOR=1

echo "Создание топиков в Kafka ($BOOTSTRAP_SERVER)"

for TOPIC in "${TOPICS[@]}"; do
    echo "Проверяем топик: $TOPIC"
    EXISTS=$(docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER | grep -x $TOPIC)
    if [ -z "$EXISTS" ]; then
        echo "Создаём топик: $TOPIC (partitions=$PARTITIONS, replication=$REPLICATION_FACTOR)"
        docker exec $KAFKA_CONTAINER kafka-topics --create \
            --topic $TOPIC \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --partitions $PARTITIONS \
            --replication-factor $REPLICATION_FACTOR
        echo "Топик $TOPIC создан"
    else
        echo "Топик $TOPIC уже существует"
    fi
done

docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER
