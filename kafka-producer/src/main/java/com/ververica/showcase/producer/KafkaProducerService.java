package com.ververica.showcase.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.showcase.producer.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core Kafka producer service.
 *
 * Implements {@link ApplicationRunner} so the produce loop starts automatically
 * after the Spring context is fully initialised (actuator endpoints are live before
 * messages start flowing, so health probes succeed from the outset).
 *
 * Producer configuration rationale:
 * - acks=1:          Leader acknowledgement only. For a throughput demo we prefer
 *                    low latency over full durability; change to "all" for production
 *                    pipelines with replication factor >= 3.
 * - linger.ms=5:     Allows batching within a 5 ms window, increasing throughput
 *                    without significant latency increase.
 * - batch.size=65536:64 KB batches align with typical network MTU and Kafka
 *                    segment sizes, improving I/O efficiency at high rates.
 * - compression=lz4: Best CPU/ratio trade-off for JSON payloads; reduces broker
 *                    storage and network bandwidth at peak.
 * - max.in.flight=5: Default; no ordering guarantee is required here because
 *                    each order is independent. Would need =1 with idempotence
 *                    if strict per-key ordering were required.
 *
 * The inner static class {@link MessageCounters} is a Spring bean so PeakSimulator
 * can read message/error counts without a circular dependency through this service.
 */
@Service
public class KafkaProducerService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    /** Interval between periodic progress log lines (milliseconds). */
    private static final long LOG_INTERVAL_MS = 10_000L;

    private final OrderGenerator orderGenerator;
    private final ThroughputController throughputController;
    private final MessageCounters counters;
    private final ObjectMapper objectMapper;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    /** Flag checked by the produce loop — set to false on @PreDestroy. */
    private volatile boolean running = true;

    public KafkaProducerService(
            OrderGenerator orderGenerator,
            ThroughputController throughputController,
            MessageCounters counters,
            KafkaProducer<String, String> producer,
            @Value("${producer.kafka.topic:orders}") String topic) {
        this.orderGenerator       = orderGenerator;
        this.throughputController = throughputController;
        this.counters             = counters;
        this.objectMapper         = new ObjectMapper();
        this.topic                = topic;
        this.producer             = producer;
        log.info("KafkaProducerService initialised — topic={}", topic);
    }

    /**
     * Entry point called by Spring Boot after context refresh.
     * Starts the produce loop on a background thread so this method returns immediately,
     * allowing Spring Boot to fire ApplicationReadyEvent and set readiness to ACCEPTING_TRAFFIC.
     */
    @Override
    public void run(ApplicationArguments args) {
        log.info("Producer loop starting on topic '{}'", topic);
        Thread producerThread = new Thread(this::produceLoop, "producer-loop");
        producerThread.setDaemon(false);
        producerThread.start();
    }

    private void produceLoop() {
        long lastLogTime  = System.currentTimeMillis();
        long lastLogCount = 0L;

        while (running) {
            try {
                Order order = orderGenerator.generate();
                String json = objectMapper.writeValueAsString(order);

                // Acquire a rate-limiter permit BEFORE sending to honour the configured rate
                throughputController.acquire();

                ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, order.getOrderId(), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        counters.incrementErrors();
                        log.error("Failed to send order {}: {}", order.getOrderId(), exception.getMessage());
                    } else {
                        counters.incrementProduced();
                    }
                });

                // Periodic progress log every LOG_INTERVAL_MS
                long now = System.currentTimeMillis();
                if (now - lastLogTime >= LOG_INTERVAL_MS) {
                    long totalNow = counters.getMessagesProduced();
                    long delta    = totalNow - lastLogCount;
                    long rate     = (delta * 1000L) / Math.max(1, now - lastLogTime);
                    log.info("Produced {} messages at {} msg/s (target: {} msg/s, errors: {})",
                             totalNow, rate, throughputController.getTargetRate(), counters.getErrorCount());
                    lastLogTime  = now;
                    lastLogCount = totalNow;
                }

            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                counters.incrementErrors();
                log.error("JSON serialisation error (this should not happen): {}", e.getMessage());
            } catch (Exception e) {
                if (running) {
                    counters.incrementErrors();
                    log.error("Unexpected error in produce loop: {}", e.getMessage(), e);
                }
            }
        }

        log.info("Producer loop exited — total messages produced: {}", counters.getMessagesProduced());
    }

    /**
     * Called by Spring on context shutdown.
     * Signals the produce loop to stop, then flushes buffered records and closes
     * the producer cleanly to avoid message loss on graceful termination.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down KafkaProducerService...");
        running = false;
        producer.flush();
        producer.close();
        log.info("KafkaProducer closed cleanly");
    }

    /**
     * Shared message counter bean.
     *
     * Extracted as a separate Spring bean so {@link PeakSimulator} can read counters
     * without depending on {@link KafkaProducerService} (which would create a
     * circular dependency through ThroughputController).
     */
    @Service
    public static class MessageCounters {

        private final AtomicLong messagesProduced = new AtomicLong(0);
        private final AtomicLong errorCount       = new AtomicLong(0);

        public void incrementProduced()       { messagesProduced.incrementAndGet(); }
        public void incrementErrors()         { errorCount.incrementAndGet(); }
        public long getMessagesProduced()     { return messagesProduced.get(); }
        public long getErrorCount()           { return errorCount.get(); }
    }
}
