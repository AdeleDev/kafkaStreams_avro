import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServersConfig = args.length != 0 && !args[0].isEmpty() ? args[0] : "localhost:9092";
        String registryUrl = args.length > 1 && !args[1].isEmpty() ? args[1] : "http://localhost:8081";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", registryUrl);
        KafkaProducer<String, User> producer = new KafkaProducer<>(props);

        User user = User.newBuilder()
                .setName("John Doe")
                .setAge(20)
                .build();

        // send data
        ProducerRecord<String, User> record = new ProducerRecord<>("users", user.getName().toString(), user);

        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                System.out.println("Sent data: " + metadata.toString());
            } else {
                System.out.println(exception.getMessage());
            }
        });
        Thread.sleep(1000L);
        producer.flush();

        producer.close();
    }
}
