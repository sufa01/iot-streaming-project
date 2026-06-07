#!/bin/bash

# Скрипт для запуска IoTEventProducer
# Генерирует события (раз в секунду) и отправляет в Kafka топик iot-events

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Параметры
MAIN_CLASS="com.iot.producer.IoTEventProducer"
KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="iot-events"

echo "Запуск IoTEventProducer из директории: $PROJECT_DIR"
echo "Kafka bootstrap: $KAFKA_BOOTSTRAP, топик: $TOPIC"

# Проверяем, собран ли проект (есть ли target/classes или jar)
if [ -f "target/iot-streaming-project-1.0-SNAPSHOT.jar" ]; then
    # Запуск из fat JAR
    java -cp "target/iot-streaming-project-1.0-SNAPSHOT.jar" $MAIN_CLASS
elif [ -d "target/classes" ]; then
    # Запуск из скомпилированных классов (например, после mvn compile)
    java -cp "target/classes:$(find ~/.m2/repository -name '*.jar' | tr '\n' ':')" $MAIN_CLASS
else
    # Сборка через Maven перед запуском
    echo "Проект не собран. Выполняем mvn compile..."
    mvn compile
    if [ $? -ne 0 ]; then
        echo "Ошибка сборки проекта."
        exit 1
    fi
    java -cp "target/classes:$(find ~/.m2/repository -name '*.jar' | tr '\n' ':')" $MAIN_CLASS
fi
