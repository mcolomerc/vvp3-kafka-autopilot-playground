package com.ververica.showcase.functions;

import com.ververica.showcase.model.Order;
import com.ververica.showcase.model.OrderAccumulator;
import com.ververica.showcase.model.OrderMetrics;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;

/**
 * Incremental {@link AggregateFunction} that accumulates {@link Order} events into an
 * {@link OrderAccumulator} and produces a partial {@link OrderMetrics} result.
 *
 * <p>The {@code windowStart}/{@code windowEnd} fields of the result are populated later by
 * {@link OrderMetricsProcessWindowFunction}, which wraps this function in a
 * {@code DataStream.aggregate(agg, windowFn)} call — the standard Flink pattern for combining
 * incremental aggregation with window metadata enrichment.
 *
 * <p>The {@code merge} method is implemented to support session windows and any future use of
 * merging windows; it is also exercised by Flink's incremental checkpoint merge paths.
 */
public class OrderMetricsAggregateFunction
        implements AggregateFunction<Order, OrderAccumulator, OrderMetrics> {

    private static final long serialVersionUID = 1L;

    // -------------------------------------------------------------------------
    // AggregateFunction contract
    // -------------------------------------------------------------------------

    @Override
    public OrderAccumulator createAccumulator() {
        // minOrderValue starts at MAX_VALUE so the first real order always wins.
        return new OrderAccumulator();
    }

    @Override
    public OrderAccumulator add(Order order, OrderAccumulator acc) {
        acc.setOrderCount(acc.getOrderCount() + 1);
        acc.setTotalRevenue(acc.getTotalRevenue() + order.getTotalAmount());

        if (order.getTotalAmount() > acc.getMaxOrderValue()) {
            acc.setMaxOrderValue(order.getTotalAmount());
        }
        if (order.getTotalAmount() < acc.getMinOrderValue()) {
            acc.setMinOrderValue(order.getTotalAmount());
        }

        acc.getCustomerIds().add(order.getCustomerId());

        // Capture the key dimensions from the first (or any) order in the window.
        // All orders in the same keyed window share the same category and region,
        // so this is idempotent.
        if (acc.getCategory() == null) {
            acc.setCategory(order.getCategory());
        }
        if (acc.getRegion() == null) {
            acc.setRegion(order.getRegion());
        }

        return acc;
    }

    @Override
    public OrderMetrics getResult(OrderAccumulator acc) {
        long count = acc.getOrderCount();
        double avgOrderValue = count > 0 ? acc.getTotalRevenue() / count : 0.0;

        // Guard against a window with zero orders (defensive — should not happen in practice).
        double minOrderValue = acc.getMinOrderValue() == Double.MAX_VALUE
                ? 0.0
                : acc.getMinOrderValue();
        double maxOrderValue = acc.getMaxOrderValue() == Double.MIN_VALUE
                ? 0.0
                : acc.getMaxOrderValue();

        // windowStart and windowEnd are injected by OrderMetricsProcessWindowFunction.
        return new OrderMetrics(
                0L,                                      // windowStart — enriched later
                0L,                                      // windowEnd   — enriched later
                acc.getCategory(),
                acc.getRegion(),
                count,
                acc.getTotalRevenue(),
                avgOrderValue,
                maxOrderValue,
                minOrderValue,
                acc.getCustomerIds().size()
        );
    }

    @Override
    public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
        // Merge b into a (Flink convention: a is the target accumulator).
        a.setOrderCount(a.getOrderCount() + b.getOrderCount());
        a.setTotalRevenue(a.getTotalRevenue() + b.getTotalRevenue());

        if (b.getMaxOrderValue() > a.getMaxOrderValue()) {
            a.setMaxOrderValue(b.getMaxOrderValue());
        }
        if (b.getMinOrderValue() < a.getMinOrderValue()) {
            a.setMinOrderValue(b.getMinOrderValue());
        }

        // Merge customer ID sets — creates a new HashSet to avoid aliasing.
        HashSet<String> merged = new HashSet<>(a.getCustomerIds());
        merged.addAll(b.getCustomerIds());
        a.setCustomerIds(merged);

        // Category and region should be identical for merges within the same keyed window,
        // but prefer non-null values defensively.
        if (a.getCategory() == null && b.getCategory() != null) {
            a.setCategory(b.getCategory());
        }
        if (a.getRegion() == null && b.getRegion() != null) {
            a.setRegion(b.getRegion());
        }

        return a;
    }
}
