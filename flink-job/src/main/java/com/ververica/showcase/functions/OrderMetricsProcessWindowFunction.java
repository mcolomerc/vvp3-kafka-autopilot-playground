package com.ververica.showcase.functions;

import com.ververica.showcase.model.OrderMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Enriches the partial {@link OrderMetrics} produced by {@link OrderMetricsAggregateFunction}
 * with the actual window start and end timestamps obtained from the {@link TimeWindow} context.
 *
 * <p>This follows the standard Flink pattern for combining incremental aggregation (low memory
 * footprint: only one accumulator per window) with per-window metadata.  The
 * {@code DataStream.aggregate(AggregateFunction, ProcessWindowFunction)} overload is used in
 * {@code OrdersJob} so that Flink first runs the incremental aggregation and passes only the
 * single {@link OrderMetrics} result into this function — no buffering of individual events.
 *
 * <p>Key type: {@code String} — the {@code category + "#" + region} key.
 */
public class OrderMetricsProcessWindowFunction
        extends ProcessWindowFunction<OrderMetrics, OrderMetrics, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            String key,
            Context context,
            Iterable<OrderMetrics> elements,
            Collector<OrderMetrics> out) {

        // There is exactly one element because we are wrapping an AggregateFunction.
        OrderMetrics partial = elements.iterator().next();

        // Inject window boundaries into the result.
        partial.setWindowStart(context.window().getStart());
        partial.setWindowEnd(context.window().getEnd());

        out.collect(partial);
    }
}
