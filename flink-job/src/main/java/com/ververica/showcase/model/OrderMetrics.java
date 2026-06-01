package com.ververica.showcase.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Aggregated metrics for a tumbling 30-second event-time window, keyed by
 * {@code category + "#" + region}.  Emitted to the Kafka topic {@code order-metrics}.
 */
public class OrderMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("windowStart")
    private long windowStart;

    @JsonProperty("windowEnd")
    private long windowEnd;

    @JsonProperty("category")
    private String category;

    @JsonProperty("region")
    private String region;

    @JsonProperty("orderCount")
    private long orderCount;

    @JsonProperty("totalRevenue")
    private double totalRevenue;

    @JsonProperty("avgOrderValue")
    private double avgOrderValue;

    @JsonProperty("maxOrderValue")
    private double maxOrderValue;

    @JsonProperty("minOrderValue")
    private double minOrderValue;

    /** Approximate distinct customer count within the window (exact, using a Set in the accumulator). */
    @JsonProperty("uniqueCustomers")
    private long uniqueCustomers;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Required by Flink's PojoSerializer and Jackson. */
    public OrderMetrics() {}

    public OrderMetrics(
            long windowStart,
            long windowEnd,
            String category,
            String region,
            long orderCount,
            double totalRevenue,
            double avgOrderValue,
            double maxOrderValue,
            double minOrderValue,
            long uniqueCustomers) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.category = category;
        this.region = region;
        this.orderCount = orderCount;
        this.totalRevenue = totalRevenue;
        this.avgOrderValue = avgOrderValue;
        this.maxOrderValue = maxOrderValue;
        this.minOrderValue = minOrderValue;
        this.uniqueCustomers = uniqueCustomers;
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public long getOrderCount() { return orderCount; }
    public void setOrderCount(long orderCount) { this.orderCount = orderCount; }

    public double getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(double totalRevenue) { this.totalRevenue = totalRevenue; }

    public double getAvgOrderValue() { return avgOrderValue; }
    public void setAvgOrderValue(double avgOrderValue) { this.avgOrderValue = avgOrderValue; }

    public double getMaxOrderValue() { return maxOrderValue; }
    public void setMaxOrderValue(double maxOrderValue) { this.maxOrderValue = maxOrderValue; }

    public double getMinOrderValue() { return minOrderValue; }
    public void setMinOrderValue(double minOrderValue) { this.minOrderValue = minOrderValue; }

    public long getUniqueCustomers() { return uniqueCustomers; }
    public void setUniqueCustomers(long uniqueCustomers) { this.uniqueCustomers = uniqueCustomers; }

    @Override
    public String toString() {
        return "OrderMetrics{" +
                "windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", category='" + category + '\'' +
                ", region='" + region + '\'' +
                ", orderCount=" + orderCount +
                ", totalRevenue=" + totalRevenue +
                ", avgOrderValue=" + avgOrderValue +
                ", uniqueCustomers=" + uniqueCustomers +
                '}';
    }
}
