#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Параметры
MAIN_CLASS="com.iot.producer.IoTEventProducer"
KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="iot-events"
echo "Kafka bootstrap: $KAFKA_BOOTSTRAP, топик: $TOPIC"
if [ -f "target/iot-streaming-project-1.0-SNAPSHOT.jar" ]; then
    java -cp "target/iot-streaming-project-1.0-SNAPSHOT.jar" $MAIN_CLASS
elif [ -d "target/classes" ]; then
    java -cp "target/classes:$(find ~/.m2/repository -name '*.jar' | tr '\n' ':')" $MAIN_CLASS
else
    mvn compile
    if [ $? -ne 0 ]; then
        echo "Ошибка сборки проекта"
        exit 1
    fi
    java -cp "target/classes:$(find ~/.m2/repository -name '*.jar' | tr '\n' ':')" $MAIN_CLASS
fi
