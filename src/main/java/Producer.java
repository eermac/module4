import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Producer.Producer с SASL/PLAIN
 */
public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC_NAME = "topic-2";

    public static void main(String[] args) {

        // Конфигурация Kafka Producer.Producer
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:19092,localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Конфигурация SASL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"admin\" password=\"admin-secret\";");

        // SSL конфигурация
        props.put("ssl.truststore.location", "./kafka-1-creds/kafka-1.truststore.jks");
        props.put("ssl.truststore.password", "password");


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "SASL/PLAIN");

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Сообщение успешно отправлено в Kafka: " + metadata.toString());
                    System.out.println(metadata);
                } else {
                    logger.error("Ошибка при отправке сообщения", exception);

                }
            });

            producer.flush();
        } catch (Throwable e) {
            logger.error("Ошибка в Kafka Producer.Producer", e);
        }
    }
}