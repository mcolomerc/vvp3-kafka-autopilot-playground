package com.ververica.showcase.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaProducer<String, String> kafkaProducer(
            @Value("${producer.kafka.bootstrap-servers:kafka:9092}") String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,             StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,           StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                             "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,                 "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG,                        "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,                       "65536");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,   "5");
        props.put(ProducerConfig.RETRIES_CONFIG,                          "3");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,                 "100");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,                    "67108864");
        return new KafkaProducer<>(props);
    }
}
