package com.ververica.showcase.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.showcase.model.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes a Kafka {@link ConsumerRecord} into an {@link Order}.
 *
 * <p>Implementation notes:
 * <ul>
 *   <li>Uses {@link KafkaRecordDeserializationSchema} (Flink 1.17+ API) which provides access to
 *       the full Kafka record including headers — aligned with the connector version 3.3.0-1.20.</li>
 *   <li>The event timestamp embedded in the record value ({@code orderTimestamp}) is used for
 *       event-time processing.  The watermark strategy is configured in {@code OrdersJob} rather
 *       than here to keep serialisation concerns separate.</li>
 *   <li>Malformed records are logged at WARN level and silently dropped (null is not collected),
 *       which prevents a single bad message from failing the job during a showcase.</li>
 *   <li>The {@link ObjectMapper} is declared {@code transient} and lazily initialised to avoid
 *       serialisation issues when Flink ships this object to task managers. This is a well-known
 *       pattern because ObjectMapper is not serialisable.</li>
 * </ul>
 */
public class OrderDeserializationSchema implements KafkaRecordDeserializationSchema<Order> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderDeserializationSchema.class);

    /** Transient: re-created on the task manager side after deserialisation. */
    private transient ObjectMapper objectMapper;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper()
                    // Tolerate unknown JSON fields so forward-compatible producer changes
                    // don't immediately break the Flink job.
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }

    // -------------------------------------------------------------------------
    // KafkaRecordDeserializationSchema
    // -------------------------------------------------------------------------

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Order> out)
            throws IOException {
        if (record.value() == null) {
            LOG.warn("Received Kafka record with null value at offset {} partition {} — skipping",
                    record.offset(), record.partition());
            return;
        }
        try {
            Order order = getObjectMapper().readValue(record.value(), Order.class);
            // Basic validation: the event timestamp must be a positive epoch-ms value.
            // The main pipeline also filters on this, but an early check here avoids
            // emitting Orders that would never contribute to any window.
            if (order.getOrderTimestamp() <= 0) {
                LOG.warn("Order {} has invalid orderTimestamp={} — skipping",
                        order.getOrderId(), order.getOrderTimestamp());
                return;
            }
            out.collect(order);
        } catch (Exception e) {
            LOG.warn("Failed to deserialise Order from Kafka record at offset {} partition {}: {}",
                    record.offset(), record.partition(), e.getMessage());
        }
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        // Use Flink's type system — avoids Kryo fallback for the POJO.
        return TypeInformation.of(Order.class);
    }
}
