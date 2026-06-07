import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class IoTEventProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random rand = new Random();
        int[] deviceTypeIds = {1,2,3,4,5};

        while (true) {
            long now = System.currentTimeMillis();
            IoTEvent event = new IoTEvent(
                deviceTypeIds[rand.nextInt(deviceTypeIds.length)],
                now,
                15.0 + rand.nextDouble() * 20,   // температура 15-35
                rand.nextInt(100)                 // влажность 0-99
            );
            String json = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>("iot-events", json));
            System.out.println("Sent: " + json);
            Thread.sleep(1000);
        }
    }

    static class IoTEvent {
        public int deviceTypeId;
        public long timestamp;
        public double temperature;
        public int humidity;

        public IoTEvent(int deviceTypeId, long timestamp, double temperature, int humidity) {
            this.deviceTypeId = deviceTypeId;
            this.timestamp = timestamp;
            this.temperature = temperature;
            this.humidity = humidity;
        }
    }
}
