import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class IoTAggregator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. Source: Kafka (как Table)
        tEnv.executeSql(
            "CREATE TABLE iot_raw (\n" +
            "  deviceTypeId INT,\n" +
            "  `timestamp` BIGINT,\n" +
            "  temperature DOUBLE,\n" +
            "  humidity INT,\n" +
            "  event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),\n" +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'iot-events',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'properties.group.id' = 'flink-group',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'format' = 'json',\n" +
            "  'json.fail-on-missing-field' = 'false',\n" +
            "  'json.ignore-parse-errors' = 'true'\n" +
            ")"
        );

        // 2. Source: PostgreSQL справочник (как временная таблица, но мы сделаем lookup join)
        // Создадим JDBC таблицу для lookup
        tEnv.executeSql(
            "CREATE TABLE device_types (\n" +
            "  id INT,\n" +
            "  type_name STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:postgresql://localhost:5432/iotdb',\n" +
            "  'table-name' = 'device_types',\n" +
            "  'username' = 'iotuser',\n" +
            "  'password' = 'iotpass',\n" +
            "  'lookup.cache.max-rows' = '100',\n" +
            "  'lookup.cache.ttl' = '1 hour'\n" +
            ")"
        );

        // 3. Join потока со справочником (обогащение)
        Table enriched = tEnv.sqlQuery(
            "SELECT \n" +
            "  i.deviceTypeId,\n" +
            "  d.type_name,\n" +
            "  i.timestamp,\n" +
            "  i.event_time,\n" +
            "  i.temperature,\n" +
            "  i.humidity\n" +
            "FROM iot_raw AS i\n" +
            "JOIN device_types FOR SYSTEM_TIME AS OF i.event_time AS d\n" +
            "ON i.deviceTypeId = d.id"
        );

        // 4. Переход из Table API в DataStream API для оконной агрегации с медианой
        DataStream<Row> enrichedStream = tEnv.toDataStream(enriched);

        // Назначаем водяные знаки и event time (из поля timestamp)
        DataStream<Row> withWatermark = enrichedStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((row, ts) -> row.getFieldAs("timestamp"))
        );

        // Агрегация: tumbling window 1 минута, считаем avg(temperature), median(humidity)
        DataStream<AggregateResult> aggregated = withWatermark
            .keyBy(row -> row.getFieldAs("deviceTypeId")) // ключ - тип устройства
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new AggregateFunction() {
                @Override
                public Object createAccumulator() { 
                    return new Acc(); 
                }
                @Override
                public Object add(Row row, Object acc) {
                    Acc a = (Acc) acc;
                    a.sumTemp += row.getFieldAs("temperature");
                    a.count++;
                    a.humidities.add(row.getFieldAs("humidity"));
                    return a;
                }
                @Override
                public Object getResult(Object acc) {
                    Acc a = (Acc) acc;
                    double avgTemp = a.sumTemp / a.count;
                    double medianHum = median(a.humidities);
                    return new AggregateResult(
                        a.deviceTypeId,
                        a.windowEnd,
                        avgTemp,
                        medianHum
                    );
                }
                @Override
                public Object merge(Object a, Object b) { return null; }
            });

        // 5. Переход обратно в Table API для отправки в Kafka
        DataStream<Row> resultRowStream = aggregated.map(agg -> {
            String timeStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(agg.windowEnd), ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("HH:mm"));
        });
    }
}
