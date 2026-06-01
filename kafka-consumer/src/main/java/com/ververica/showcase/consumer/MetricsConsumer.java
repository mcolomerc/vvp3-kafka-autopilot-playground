package com.ververica.showcase.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.showcase.consumer.model.OrderMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight Kafka consumer that reads {@link OrderMetrics} windows from the
 * "order-metrics" topic and logs each record as a structured line.
 *
 * This serves as the end-to-end sink in the VVP3 Autopilot demo, confirming that
 * orders produced → Flink processed → metrics published flow is working.
 *
 * Consumer configuration rationale:
 * - enable.auto.commit=true / interval=5000ms: Acceptable here because this is a
 *   read-only logging sink. At-most-once loss of a small summary window is
 *   tolerable; the Flink job retains exactly-once semantics upstream.
 * - auto.offset.reset=earliest: On first run the consumer starts from the oldest
 *   available offsets so no historical windows are missed during demo setup.
 * - max.poll.records=500: Caps the batch size per poll to prevent long processing
 *   pauses that could trigger a session timeout.
 * - session.timeout.ms (default 45s) and heartbeat.interval.ms (default 3s) are
 *   left at Kafka broker defaults — appropriate for a low-latency consumer.
 *
 * Graceful shutdown:
 *   A JVM shutdown hook calls consumer.wakeup() which causes the poll() call to
 *   throw WakeupException. The main loop catches this, closes the consumer, and exits.
 */
public class MetricsConsumer {

    private static final Logger log = LoggerFactory.getLogger(MetricsConsumer.class);

    /** How often to emit a summary log line (milliseconds). */
    private static final long SUMMARY_INTERVAL_MS = 60_000L;

    public static void main(String[] args) {
        // --- Read configuration from environment variables with sensible defaults ---
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic            = getEnv("KAFKA_TOPIC_METRICS",     "order-metrics");
        String groupId          = getEnv("KAFKA_CONSUMER_GROUP",    "metrics-logger");

        log.info("MetricsConsumer starting — bootstrap={} topic={} group={}",
                 bootstrapServers, topic, groupId);

        Properties props = buildConsumerProperties(bootstrapServers, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        // Register shutdown hook to wake the consumer on SIGTERM
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — waking consumer");
            consumer.wakeup();
            try {
                mainThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown-hook"));

        AtomicLong totalWindowsProcessed = new AtomicLong(0);
        long lastWindowEnd    = 0L;
        long lastSummaryTime  = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("Subscribed to topic '{}'", topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        OrderMetrics metrics = objectMapper.readValue(record.value(), OrderMetrics.class);

                        // Structured log line — one per window/category/region combination
                        log.info("[window={}-{}] category={} region={} orders={} revenue={} avg={} customers={}",
                                 metrics.getWindowStart(),
                                 metrics.getWindowEnd(),
                                 metrics.getCategory(),
                                 metrics.getRegion(),
                                 metrics.getOrderCount(),
                                 String.format("%.2f", metrics.getTotalRevenue()),
                                 String.format("%.2f", metrics.getAvgOrderValue()),
                                 metrics.getUniqueCustomers());

                        totalWindowsProcessed.incrementAndGet();
                        if (metrics.getWindowEnd() > lastWindowEnd) {
                            lastWindowEnd = metrics.getWindowEnd();
                        }

                    } catch (Exception e) {
                        log.error("Failed to deserialize record at offset {} on partition {}: {}",
                                  record.offset(), record.partition(), e.getMessage());
                    }
                }

                // Periodic summary log every SUMMARY_INTERVAL_MS
                long now = System.currentTimeMillis();
                if (now - lastSummaryTime >= SUMMARY_INTERVAL_MS) {
                    log.info("[SUMMARY] Total windows processed: {} | Last window: {}",
                             totalWindowsProcessed.get(), lastWindowEnd);
                    lastSummaryTime = now;
                }
            }

        } catch (WakeupException e) {
            // Expected on shutdown — not an error
            log.info("Consumer woken up for shutdown");
        } finally {
            try {
                consumer.close();
                log.info("KafkaConsumer closed cleanly — total windows processed: {}",
                         totalWindowsProcessed.get());
            } catch (Exception e) {
                log.error("Error closing consumer: {}", e.getMessage());
            }
        }
    }

    private static Properties buildConsumerProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        // Auto-commit is acceptable for this logging-only sink
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,  "5000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Limit batch size to keep processing loop responsive
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         "500");
        return props;
    }

    /** Reads an environment variable, returning {@code defaultValue} if not set or blank. */
    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
