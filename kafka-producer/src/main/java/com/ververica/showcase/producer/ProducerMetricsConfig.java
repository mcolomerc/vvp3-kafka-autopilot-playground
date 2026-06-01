package com.ververica.showcase.producer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * Binds two metric sources to the Micrometer registry so they are
 * available at /actuator/prometheus for Prometheus scraping:
 *
 * 1. KafkaClientMetrics — wraps the KafkaProducer's internal MetricRegistry and
 *    publishes all kafka.producer.* metrics (send rate, error rate, request
 *    latency, batch size, buffer usage, etc.).
 *
 * 2. Application gauges — current/base throughput, peak state, and counters
 *    sourced from ThroughputController, PeakSimulator, and MessageCounters.
 *    These reflect the values visible on GET /api/status.
 */
@Configuration
public class ProducerMetricsConfig {

    private final MeterRegistry registry;
    private final KafkaProducer<?, ?> producer;
    private final ThroughputController throughputController;
    private final PeakSimulator peakSimulator;
    private final KafkaProducerService.MessageCounters counters;

    public ProducerMetricsConfig(
            MeterRegistry registry,
            KafkaProducer<?, ?> producer,
            ThroughputController throughputController,
            PeakSimulator peakSimulator,
            KafkaProducerService.MessageCounters counters) {
        this.registry             = registry;
        this.producer             = producer;
        this.throughputController = throughputController;
        this.peakSimulator        = peakSimulator;
        this.counters             = counters;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void bindMetrics() {
        // Kafka client metrics (record-send-rate, record-error-rate, request-latency-avg, etc.)
        new KafkaClientMetrics(producer).bindTo(registry);

        // Application-level gauges
        Gauge.builder("producer.throughput.current", throughputController, ThroughputController::getTargetRate)
             .description("Current message send rate (msg/s)")
             .register(registry);

        Gauge.builder("producer.throughput.base", peakSimulator, PeakSimulator::getBaseThroughput)
             .description("Configured base throughput (msg/s)")
             .register(registry);

        Gauge.builder("producer.peak.active", peakSimulator, s -> s.isPeakActive() ? 1.0 : 0.0)
             .description("1 when a peak simulation is running, 0 otherwise")
             .register(registry);

        Gauge.builder("producer.messages.produced", counters, c -> (double) c.getMessagesProduced())
             .description("Total messages successfully sent since startup")
             .register(registry);

        Gauge.builder("producer.messages.errors", counters, c -> (double) c.getErrorCount())
             .description("Total send errors since startup")
             .register(registry);
    }
}
