package com.ververica.showcase.consumer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an aggregated order-metrics window record produced by the Flink pipeline
 * and consumed from the Kafka "order-metrics" topic.
 *
 * {@code @JsonIgnoreProperties(ignoreUnknown = true)} makes the consumer forward-compatible:
 * new fields added to the Flink output will not cause deserialization failures.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderMetrics {

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

    @JsonProperty("uniqueCustomers")
    private long uniqueCustomers;

    // No-arg constructor required by Jackson
    public OrderMetrics() {
    }

    public OrderMetrics(long windowStart, long windowEnd, String category, String region,
                        long orderCount, double totalRevenue, double avgOrderValue,
                        double maxOrderValue, double minOrderValue, long uniqueCustomers) {
        this.windowStart     = windowStart;
        this.windowEnd       = windowEnd;
        this.category        = category;
        this.region          = region;
        this.orderCount      = orderCount;
        this.totalRevenue    = totalRevenue;
        this.avgOrderValue   = avgOrderValue;
        this.maxOrderValue   = maxOrderValue;
        this.minOrderValue   = minOrderValue;
        this.uniqueCustomers = uniqueCustomers;
    }

    public long   getWindowStart()     { return windowStart; }
    public void   setWindowStart(long windowStart)       { this.windowStart = windowStart; }

    public long   getWindowEnd()       { return windowEnd; }
    public void   setWindowEnd(long windowEnd)           { this.windowEnd = windowEnd; }

    public String getCategory()        { return category; }
    public void   setCategory(String category)           { this.category = category; }

    public String getRegion()          { return region; }
    public void   setRegion(String region)               { this.region = region; }

    public long   getOrderCount()      { return orderCount; }
    public void   setOrderCount(long orderCount)         { this.orderCount = orderCount; }

    public double getTotalRevenue()    { return totalRevenue; }
    public void   setTotalRevenue(double totalRevenue)   { this.totalRevenue = totalRevenue; }

    public double getAvgOrderValue()   { return avgOrderValue; }
    public void   setAvgOrderValue(double avgOrderValue) { this.avgOrderValue = avgOrderValue; }

    public double getMaxOrderValue()   { return maxOrderValue; }
    public void   setMaxOrderValue(double maxOrderValue) { this.maxOrderValue = maxOrderValue; }

    public double getMinOrderValue()   { return minOrderValue; }
    public void   setMinOrderValue(double minOrderValue) { this.minOrderValue = minOrderValue; }

    public long   getUniqueCustomers() { return uniqueCustomers; }
    public void   setUniqueCustomers(long uniqueCustomers) { this.uniqueCustomers = uniqueCustomers; }
}
