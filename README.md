# IoT Streaming Project

Генерация событий IoT-устройств (температура, влажность) в Kafka, обогащение через PostgreSQL справочник типов устройств, агрегация по минутным окнам (средняя температура, медиана влажности) и запись результата обратно в Kafka.

## Архитектура

- **Источник данных**: генератор событий (Java producer) отправляет JSON в Kafka topic `iot-events`.
- **Справочник**: PostgreSQL таблица `device_types` (id, type_name).
- **Обработка**: Apache Flink (Table API + DataStream API):
  - Чтение из Kafka (Table API).
  - Lookup join со справочником PostgreSQL (JDBC connector).
  - Переход в DataStream API для оконной агрегации (tumbling window, 1 минута).
  - Агрегация: средняя температура, медиана влажности.
  - Форматирование результата: `Время (HH:mm), Тип устройства, Средняя температура, Медиана влажности`.
- **Синк**: результат в Kafka topic `iot-aggregates`.

## Структура проекта
iot-streaming-project/
├── pom.xml
├── docker-compose.yml
├── sql/
│   ├── ddl.sql
│   └── dml.sql
├── src/
│   └── main/
│       ├── java/com/iot/
│       │   ├── producer/IoTEventProducer.java
│       │   ├── aggregator/IoTAggregator.java
│       │   └── model/
│       │       ├── IoTEvent.java
│       │       ├── EnrichedEvent.java
│       │       └── AggregateResult.java
│       └── resources/log4j.properties
├── scripts/
│   ├── create-topics.sh
│   └── run-producer.sh
└── README.md

## Требования
- Docker и Docker Compose
- Java 11 или 17
- Apache Maven

## Запуск

### 1. Поднять инфраструктуру
```bash
docker-compose up -d
```
### 2. Создать топики Kafka
```bash
chmod +x scripts/create-topics.sh
./scripts/create-topics.sh
```
Топики: `iot-events`, `iot-aggregates`.

### 3. Подготовить PostgreSQL справочник
```bash
docker exec -i postgres psql -U iotuser -d iotdb < sql/ddl.sql
docker exec -i postgres psql -U iotuser -d iotdb < sql/dml.sql
```
Проверка:

```bash
docker exec -it postgres psql -U iotuser -d iotdb -c "SELECT * FROM device_types;"
```

### 4. Собрать проект
```bash
mvn clean package
```

### 5. Запустить генератор IoT событий
В отдельном терминале:
```bash
chmod +x scripts/run-producer.sh
./scripts/run-producer.sh
```

### 6. Запустить Flink-приложение
**Через IDE** – запустите `com.iot.aggregator.IoTAggregator`.
**Через Flink CLI**:
```bash
flink run -c com.iot.aggregator.IoTAggregator target/iot-streaming-project-1.0-SNAPSHOT.jar
```

**Через Maven exec plugin**:
```bash
mvn exec:java -Dexec.mainClass="com.iot.aggregator.IoTAggregator"
```

### 7. Просмотр результата
```bash
docker exec -it kafka kafka-console-consumer --topic iot-aggregates --bootstrap-server localhost:9092 --from-beginning
```
