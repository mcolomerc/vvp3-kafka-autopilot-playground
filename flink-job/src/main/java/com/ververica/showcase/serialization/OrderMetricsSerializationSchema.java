package com.ververica.showcase.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.showcase.model.OrderMetrics;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Serialises an {@link OrderMetrics} to a Kafka {@link ProducerRecord}.
 *
 * <p>Implementation notes:
 * <ul>
 *   <li>The message key is {@code category + "#" + region} as UTF-8 bytes.  This co-locates all
 *       metrics for the same category/region onto the same Kafka partition, which is useful for
 *       downstream consumers that materialise running aggregates.</li>
 *   <li>The message value is the JSON representation of the full {@link OrderMetrics} object.</li>
 *   <li>Like {@link OrderDeserializationSchema}, the {@link ObjectMapper} is {@code transient} and
 *       lazily initialised to avoid serialisation issues during Flink operator shipping.</li>
 *   <li>On serialisation failure the record is dropped and a WARN is logged, keeping the pipeline
 *       running during the showcase without masking silent data loss in production (where you would
 *       route to a dead-letter topic instead).</li>
 * </ul>
 */
public class OrderMetricsSerializationSchema implements KafkaRecordSerializationSchema<OrderMetrics> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderMetricsSerializationSchema.class);

    private final String topic;

    /** Transient: re-created on the task manager side after deserialisation. */
    private transient ObjectMapper objectMapper;

    public OrderMetricsSerializationSchema(String topic) {
        this.topic = topic;
    }

    // -------------------------------------------------------------------------
    // Lazy initialisation of non-serialisable state
    // -------------------------------------------------------------------------

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    // -------------------------------------------------------------------------
    // KafkaRecordSerializationSchema
    // -------------------------------------------------------------------------

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            OrderMetrics metrics,
            KafkaSinkContext context,
            Long timestamp) {

        if (metrics == null) {
            return null;
        }

        // Key: category#region — ensures Kafka partition locality for the same window key.
        byte[] key = (metrics.getCategory() + "#" + metrics.getRegion())
                .getBytes(StandardCharsets.UTF_8);

        byte[] value;
        try {
            value = getObjectMapper().writeValueAsBytes(metrics);
        } catch (Exception e) {
            LOG.warn("Failed to serialise OrderMetrics for category={} region={}: {}",
                    metrics.getCategory(), metrics.getRegion(), e.getMessage());
            return null;
        }

        return new ProducerRecord<>(topic, key, value);
    }
}
