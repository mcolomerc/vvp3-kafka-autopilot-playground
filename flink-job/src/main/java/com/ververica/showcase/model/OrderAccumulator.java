package com.ververica.showcase.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Mutable accumulator used by {@link com.ververica.showcase.functions.OrderMetricsAggregateFunction}.
 *
 * <p>Design notes:
 * <ul>
 *   <li>{@code minOrderValue} is initialised to {@link Double#MAX_VALUE} so the first real
 *       value always replaces it correctly.</li>
 *   <li>{@code customerIds} is a {@link HashSet} — exact distinct count within a window.
 *       For very high-cardinality windows a HyperLogLog sketch could replace this, but for the
 *       30-second tumbling windows in this job the Set size stays manageable and the exact count
 *       is more useful for validation.</li>
 *   <li>This class is {@link Serializable} so Flink's Kryo / Java serialisation can handle it
 *       inside RocksDB or heap-based state backends without additional TypeSerializer registration.</li>
 * </ul>
 */
public class OrderAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private long orderCount;
    private double totalRevenue;
    private double maxOrderValue;
    private double minOrderValue;
    private Set<String> customerIds;
    private String category;
    private String region;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public OrderAccumulator() {
        this.orderCount = 0L;
        this.totalRevenue = 0.0;
        this.maxOrderValue = Double.MIN_VALUE;
        this.minOrderValue = Double.MAX_VALUE;
        this.customerIds = new HashSet<>();
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    public long getOrderCount() { return orderCount; }
    public void setOrderCount(long orderCount) { this.orderCount = orderCount; }

    public double getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(double totalRevenue) { this.totalRevenue = totalRevenue; }

    public double getMaxOrderValue() { return maxOrderValue; }
    public void setMaxOrderValue(double maxOrderValue) { this.maxOrderValue = maxOrderValue; }

    public double getMinOrderValue() { return minOrderValue; }
    public void setMinOrderValue(double minOrderValue) { this.minOrderValue = minOrderValue; }

    public Set<String> getCustomerIds() { return customerIds; }
    public void setCustomerIds(Set<String> customerIds) { this.customerIds = customerIds; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    @Override
    public String toString() {
        return "OrderAccumulator{" +
                "orderCount=" + orderCount +
                ", totalRevenue=" + totalRevenue +
                ", category='" + category + '\'' +
                ", region='" + region + '\'' +
                ", uniqueCustomers=" + (customerIds != null ? customerIds.size() : 0) +
                '}';
    }
}
