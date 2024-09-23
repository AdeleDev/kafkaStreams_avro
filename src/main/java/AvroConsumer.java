import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumer {

    public static void main(String[] args) {
        String bootstrapServersConfig = args.length != 0 && !args[0].isEmpty() ? args[0] : "localhost:9092";
        String registryUrl = args.length > 1 && !args[1].isEmpty() ? args[1] : "http://localhost:8081";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", registryUrl);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);

        // subscription
        try (consumer) {
            consumer.subscribe(Collections.singletonList("users"));
            while (true) {
                // read from topic
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));

                for (var record : records) {
                    System.out.println("Got message: " + record.value());

                    // offset
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, "Offset commit");
                    consumer.commitSync(Collections.singletonMap(partition, offsetAndMetadata));
                }
            }
        }
    }
}
