package com.ververica.showcase;

import com.ververica.showcase.functions.OrderMetricsAggregateFunction;
import com.ververica.showcase.functions.OrderMetricsProcessWindowFunction;
import com.ververica.showcase.model.Order;
import com.ververica.showcase.model.OrderMetrics;
import com.ververica.showcase.serialization.OrderDeserializationSchema;
import com.ververica.showcase.serialization.OrderMetricsSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point for the VVP3 Autopilot showcase Flink job.
 *
 * <h2>Pipeline overview</h2>
 * <pre>
 *   KafkaSource&lt;Order&gt; (topic: orders)
 *     → WatermarkStrategy (bounded out-of-orderness 5 s, event-time from orderTimestamp)
 *     → filter(orderTimestamp &gt; 0)                  // drop malformed records
 *     → keyBy(category + "#" + region)              // 30 partitions: 6 × 5
 *     → TumblingEventTimeWindows(30 s)
 *     → aggregate(OrderMetricsAggregateFunction,
 *                 OrderMetricsProcessWindowFunction) // incremental + window metadata
 *     → KafkaSink&lt;OrderMetrics&gt; (topic: order-metrics, AT_LEAST_ONCE)
 * </pre>
 *
 * <h2>Configuration</h2>
 * All tuneable parameters are read from environment variables with sensible defaults so the job
 * runs both locally and inside VVP3 without code changes:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS} — Kafka broker list (default: {@code kafka.kafka.svc.cluster.local:9092})</li>
 *   <li>{@code FLINK_PARALLELISM} — global job parallelism (default: {@code 1})</li>
 * </ul>
 *
 * <h2>VVP3 Autopilot compatibility</h2>
 * Every stateful operator carries a stable {@code .uid()} so that VVP3 Autopilot can resume from
 * a savepoint after a scale-out/scale-in operation without state loss.
 */
public class OrdersJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrdersJob.class);

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    static final String SOURCE_TOPIC          = "orders";
    static final String SINK_TOPIC            = "order-metrics";
    static final String CONSUMER_GROUP_ID     = "flink-orders-consumer";

    private static final long   CHECKPOINT_INTERVAL_MS = 60_000L;       // 60 s
    private static final long   CHECKPOINT_TIMEOUT_MS  = 300_000L;      // 5 min
    private static final long   CHECKPOINT_MIN_PAUSE_MS = 30_000L;      // 30 s
    private static final int    MAX_CONCURRENT_CHECKPOINTS = 1;
    private static final int    WINDOW_SIZE_SECONDS    = 30;
    private static final int    WATERMARK_LAG_SECONDS  = 5;
    private static final int    RESTART_ATTEMPTS       = 3;
    private static final long   RESTART_DELAY_MS       = 10_000L;       // 10 s

    // -------------------------------------------------------------------------
    // Main entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092");

        // Parallelism is controlled by VVP3 via the deployment's parallelism setting,
        // which sets parallelism.default in Flink cluster config.
        // We do NOT call env.setParallelism() here so VVP3 Autopilot can adjust it.
        LOG.info("Starting OrdersJob — bootstrapServers={}", bootstrapServers);

        StreamExecutionEnvironment env = buildEnvironment();
        buildPipeline(env, bootstrapServers, SINK_TOPIC);
        env.execute("VVP3 Autopilot Showcase — Orders Job");
    }

    // -------------------------------------------------------------------------
    // Environment configuration (package-private for testing)
    // -------------------------------------------------------------------------

    /**
     * Configures a {@link StreamExecutionEnvironment} with checkpointing, restart strategy,
     * and Prometheus metrics.  Extracted so tests can pass a MiniCluster environment instead.
     */
    static StreamExecutionEnvironment buildEnvironment() {
        // Provide Flink configuration inline — avoids relying on flink-conf.yaml being present.
        Configuration config = new Configuration();

        // Prometheus metrics reporter for local/standalone execution.
        // When running on VVP3, metrics.reporters is controlled by the cluster config
        // (GlobalConfiguration from flink-conf.yaml) and overrides these settings.
        // Apply the VVP3 3.1 bug workaround via the deployment's flinkConfiguration:
        //   metrics.reporter.promappmgr.factory.class: ...PrometheusReporterFactory
        //   metrics.reporter.jmx.factory.class: ...JMXReporterFactory
        config.setString("metrics.reporter.prom.factory.class",
                "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
        config.setString("metrics.reporter.prom.port", "9249");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        // Parallelism is intentionally NOT set here.
        // VVP3 controls it via the deployment's parallelism setting → parallelism.default,
        // allowing Autopilot to scale the job up and down dynamically.

        // ---- Checkpointing -----------------------------------------------------
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CHECKPOINT_MIN_PAUSE_MS);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS);
        // Retain the last checkpoint on job cancellation so VVP3 can resume from it.
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ---- State backend ------------------------------------------------------
        // HashMapStateBackend keeps state on the JVM heap; checkpoints are written to
        // the checkpoint storage configured in flink-conf.yaml (typically S3 / PVC in VVP3).
        env.setStateBackend(new HashMapStateBackend());

        // ---- Restart strategy --------------------------------------------------
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        RESTART_ATTEMPTS,
                        org.apache.flink.api.common.time.Time.milliseconds(RESTART_DELAY_MS)));

        return env;
    }

    // -------------------------------------------------------------------------
    // Pipeline construction (package-private for testing)
    // -------------------------------------------------------------------------

    /**
     * Attaches the full source → transform → sink pipeline to {@code env}.
     *
     * @param env              the execution environment (real or MiniCluster)
     * @param bootstrapServers Kafka broker addresses
     * @param sinkTopic        Kafka topic to write {@link OrderMetrics} to
     */
    static DataStream<OrderMetrics> buildPipeline(
            StreamExecutionEnvironment env,
            String bootstrapServers,
            String sinkTopic) {

        // ---- Source ------------------------------------------------------------
        KafkaSource<Order> kafkaSource = KafkaSource.<Order>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        // Fall back to earliest if no committed offset exists for the group.
                        OffsetResetStrategy.EARLIEST))
                .setDeserializer(new OrderDeserializationSchema())
                // Propagate idle partitions so watermarks advance even when some partitions
                // are quiet — critical for correct event-time windowing with low volume.
                .setProperty("partition.discovery.interval.ms", "30000")
                .build();

        // ---- Watermark strategy -----------------------------------------------
        WatermarkStrategy<Order> watermarkStrategy =
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                                Duration.ofSeconds(WATERMARK_LAG_SECONDS))
                        .withTimestampAssigner((order, recordTimestamp) ->
                                order.getOrderTimestamp())
                        // Mark a source partition as idle after 10 s of no activity so
                        // the watermark can advance past stalled partitions.
                        .withIdleness(Duration.ofSeconds(10));

        // ---- Transform ---------------------------------------------------------
        DataStream<OrderMetrics> metricsStream = env
                .fromSource(kafkaSource, watermarkStrategy, "kafka-orders-source")
                // Assign a UID to the source operator for savepoint compatibility.
                .uid("kafka-orders-source")
                .name("kafka-orders-source")

                // Drop any record that slipped through deserialisation with a bad timestamp.
                .filter(order -> order.getOrderTimestamp() > 0)
                .uid("filter-valid-timestamp")
                .name("filter-valid-timestamp")

                // Simulate CPU-intensive per-record enrichment (scoring, feature extraction, etc.)
                // Math.sin/cos costs ~20-50 ns each; 5,000 iterations ≈ 200-250 µs per record.
                // At 2,500 records/s per subtask this creates ~60-80% busy time, triggering Autopilot.
                .map(order -> {
                    double v = order.getTotalAmount();
                    for (int i = 0; i < 5000; i++) {
                        v = Math.sin(v + i) * Math.cos(v - i);
                    }
                    // Result discarded — work is purely for CPU saturation.
                    return order;
                })
                .uid("cpu-load-simulation")
                .name("cpu-load-simulation")

                // Key by category + region — produces 30 logical partitions (6 × 5).
                .keyBy(order -> order.getCategory() + "#" + order.getRegion())

                // 30-second tumbling event-time windows.
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))

                // Incremental aggregation (low memory) + window metadata enrichment.
                .aggregate(
                        new OrderMetricsAggregateFunction(),
                        new OrderMetricsProcessWindowFunction())
                .uid("orders-window-aggregation")
                .name("order-metrics-aggregation");

        // ---- Sink --------------------------------------------------------------
        KafkaSink<OrderMetrics> kafkaSink = KafkaSink.<OrderMetrics>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new OrderMetricsSerializationSchema(sinkTopic))
                // AT_LEAST_ONCE avoids the overhead of two-phase commit and is sufficient
                // for the showcase; downstream consumers are idempotent on window results.
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        metricsStream
                .sinkTo(kafkaSink)
                .uid("kafka-order-metrics-sink")
                .name("kafka-order-metrics-sink");

        return metricsStream;
    }
}
