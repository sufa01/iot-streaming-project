#!/bin/bash

# Скрипт для создания топиков Kafka
# Предполагается, что Kafka запущена через docker-compose (сервер на localhost:9092)

KAFKA_CONTAINER="kafka"          # Имя контейнера Kafka из docker-compose
BOOTSTRAP_SERVER="localhost:9092"
TOPICS=("iot-events" "iot-aggregates")
PARTITIONS=1
REPLICATION_FACTOR=1

echo "Создание топиков в Kafka ($BOOTSTRAP_SERVER)..."

for TOPIC in "${TOPICS[@]}"; do
    echo "Проверяем топик: $TOPIC"
    
    # Проверяем, существует ли уже топик
    EXISTS=$(docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER | grep -x $TOPIC)
    
    if [ -z "$EXISTS" ]; then
        echo "Создаём топик: $TOPIC (partitions=$PARTITIONS, replication=$REPLICATION_FACTOR)"
        docker exec $KAFKA_CONTAINER kafka-topics --create \
            --topic $TOPIC \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --partitions $PARTITIONS \
            --replication-factor $REPLICATION_FACTOR
        echo "Топик $TOPIC создан."
    else
        echo "Топик $TOPIC уже существует. Пропускаем."
    fi
done

echo "Готово. Топики:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER
