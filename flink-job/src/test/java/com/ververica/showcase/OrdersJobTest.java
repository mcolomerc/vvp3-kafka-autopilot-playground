package com.ververica.showcase;

import com.ververica.showcase.functions.OrderMetricsAggregateFunction;
import com.ververica.showcase.functions.OrderMetricsProcessWindowFunction;
import com.ververica.showcase.model.Order;
import com.ververica.showcase.model.OrderAccumulator;
import com.ververica.showcase.model.OrderMetrics;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link OrdersJob} using an embedded Flink MiniCluster.
 *
 * <p>Strategy:
 * <ol>
 *   <li>Generate 10 {@link Order} objects with known values within the same 30-second window.</li>
 *   <li>Drive all timestamps into the same tumbling window [0, 30 000 ms) and trigger it by
 *       appending a sentinel order with timestamp 40 000 ms (advances watermark past the boundary
 *       given the 5 s lag).</li>
 *   <li>Assert that a {@link OrderMetrics} record with {@code orderCount == 10} is produced with
 *       the expected aggregate values.</li>
 * </ol>
 *
 * <p>The Kafka source and sink are replaced with an in-memory {@code fromCollection} source and a
 * static {@link CollectSink}, avoiding the need for an embedded Kafka broker.
 *
 * <p>Uses the JUnit 4-style {@code @ClassRule} ({@link MiniClusterWithClientResource}) because
 * Flink 1.20's {@code flink-test-utils} still exposes it as the primary integration-test entry
 * point.  JUnit 5 {@code @Test} methods are driven by JUnit 5 but the cluster lifecycle is
 * managed by the JUnit 4 rule bridge — this is the officially supported pattern in Flink 1.20.
 */
public class OrdersJobTest {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(4)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeEach
    void clearSink() {
        CollectSink.values.clear();
    }

    // -------------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------------

    /**
     * End-to-end test: 10 orders in the same 30-second tumbling window should produce
     * one {@link OrderMetrics} record with the correct aggregated values.
     */
    @Test
    void testAggregationProducesCorrectMetrics() throws Exception {
        List<Order> orders = buildTestOrders();

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<Order> wm =
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((o, ts) -> o.getOrderTimestamp());

        DataStream<OrderMetrics> result = env
                .fromCollection(orders)
                .assignTimestampsAndWatermarks(wm)
                .filter(o -> o.getOrderTimestamp() > 0)
                .keyBy(o -> o.getCategory() + "#" + o.getRegion())
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .aggregate(
                        new OrderMetricsAggregateFunction(),
                        new OrderMetricsProcessWindowFunction());

        result.addSink(new CollectSink());

        env.execute("OrdersJobTest");

        List<OrderMetrics> captured = new ArrayList<>(CollectSink.values);

        // The 10 real orders are in the [0, 30 000) window.
        // The sentinel order (ts = 40 000) may produce a second record in the [30 000, 60 000)
        // window — we only care about the one with orderCount == 10.
        OrderMetrics windowResult = captured.stream()
                .filter(m -> m.getOrderCount() == 10)
                .findFirst()
                .orElse(null);

        assertNotNull(windowResult,
                "Expected a window result with orderCount=10; got records: " + captured);

        // All 10 orders have totalAmount = 100.0 → totalRevenue = 1000.0
        assertEquals(10L, windowResult.getOrderCount(), "orderCount mismatch");
        assertEquals(1000.0, windowResult.getTotalRevenue(), 0.001, "totalRevenue mismatch");
        assertEquals(100.0, windowResult.getAvgOrderValue(), 0.001, "avgOrderValue mismatch");
        assertEquals(100.0, windowResult.getMaxOrderValue(), 0.001, "maxOrderValue mismatch");
        assertEquals(100.0, windowResult.getMinOrderValue(), 0.001, "minOrderValue mismatch");

        // Customers: CUST-00002 … CUST-00006 (indices 1–10, modulo 5 + 1) = 5 unique
        assertEquals(5L, windowResult.getUniqueCustomers(), "uniqueCustomers mismatch");

        assertEquals("ELECTRONICS", windowResult.getCategory(), "category mismatch");
        assertEquals("NORTH", windowResult.getRegion(), "region mismatch");

        // Window boundaries must be populated by the ProcessWindowFunction.
        assertEquals(0L, windowResult.getWindowStart(), "windowStart should be 0");
        assertEquals(30_000L, windowResult.getWindowEnd(), "windowEnd should be 30000");
    }

    // -------------------------------------------------------------------------
    // Unit tests (no MiniCluster required)
    // -------------------------------------------------------------------------

    /** Verifies incremental accumulation logic of {@link OrderMetricsAggregateFunction}. */
    @Test
    void testUnitAggregateFunction() {
        OrderMetricsAggregateFunction fn = new OrderMetricsAggregateFunction();
        OrderAccumulator acc = fn.createAccumulator();

        Order o1 = makeOrder("CUST-00001", "ELECTRONICS", "NORTH",  50.0, 1000L);
        Order o2 = makeOrder("CUST-00002", "ELECTRONICS", "NORTH", 150.0, 2000L);
        Order o3 = makeOrder("CUST-00001", "ELECTRONICS", "NORTH", 100.0, 3000L);

        acc = fn.add(o1, acc);
        acc = fn.add(o2, acc);
        acc = fn.add(o3, acc);

        OrderMetrics result = fn.getResult(acc);

        assertEquals(3L, result.getOrderCount());
        assertEquals(300.0, result.getTotalRevenue(), 0.001);
        assertEquals(100.0, result.getAvgOrderValue(), 0.001);
        assertEquals(150.0, result.getMaxOrderValue(), 0.001);
        assertEquals(50.0,  result.getMinOrderValue(), 0.001);
        // CUST-00001 appears twice but should count as 1 unique.
        assertEquals(2L, result.getUniqueCustomers());
    }

    /** Verifies that two accumulators merge correctly (needed for session-window merges). */
    @Test
    void testMergeAccumulators() {
        OrderMetricsAggregateFunction fn = new OrderMetricsAggregateFunction();
        OrderAccumulator acc1 = fn.createAccumulator();
        OrderAccumulator acc2 = fn.createAccumulator();

        fn.add(makeOrder("CUST-A", "FOOD", "SOUTH", 200.0, 1000L), acc1);
        fn.add(makeOrder("CUST-B", "FOOD", "SOUTH", 100.0, 2000L), acc2);
        fn.add(makeOrder("CUST-A", "FOOD", "SOUTH", 300.0, 3000L), acc2);

        OrderAccumulator merged = fn.merge(acc1, acc2);
        OrderMetrics result = fn.getResult(merged);

        assertEquals(3L, result.getOrderCount());
        assertEquals(600.0, result.getTotalRevenue(), 0.001);
        assertEquals(300.0, result.getMaxOrderValue(), 0.001);
        assertEquals(100.0, result.getMinOrderValue(), 0.001);
        // CUST-A appears in both accumulators — should still count as 2 unique.
        assertEquals(2L, result.getUniqueCustomers());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds 10 orders in [0, 30 000) ms plus one sentinel at 40 000 ms to advance the watermark
     * past the window boundary (watermark = 40 000 - 5 000 lag = 35 000 &gt; 30 000).
     */
    private static List<Order> buildTestOrders() {
        List<Order> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            // Customers cycle through CUST-00002 … CUST-00006 (5 distinct values)
            String custId = String.format("CUST-%05d", (i % 5) + 1);
            list.add(makeOrder(custId, "ELECTRONICS", "NORTH", 100.0, (long) i * 1_000));
        }
        // Sentinel: timestamp = 40 000 ms → watermark advances to 35 000 ms, closing the window.
        list.add(makeOrder("CUST-99999", "ELECTRONICS", "NORTH", 1.0, 40_000L));
        return list;
    }

    private static Order makeOrder(
            String customerId,
            String category,
            String region,
            double totalAmount,
            long orderTimestamp) {
        return new Order(
                "order-" + orderTimestamp,
                customerId,
                "PROD-00001",
                "Test Product",
                category,
                region,
                1,
                totalAmount,
                totalAmount,
                orderTimestamp,
                System.currentTimeMillis()
        );
    }

    // -------------------------------------------------------------------------
    // Collect sink
    // -------------------------------------------------------------------------

    /**
     * Thread-safe in-memory sink that captures all emitted {@link OrderMetrics} records.
     * The static list is cleared in {@link #clearSink()} before each test.
     */
    public static class CollectSink implements SinkFunction<OrderMetrics> {

        private static final long serialVersionUID = 1L;

        // synchronized list required: Flink may call invoke() from multiple parallel subtasks.
        public static final List<OrderMetrics> values =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(OrderMetrics value, Context context) {
            values.add(value);
        }
    }
}
