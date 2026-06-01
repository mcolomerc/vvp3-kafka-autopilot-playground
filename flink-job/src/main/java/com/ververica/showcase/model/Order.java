package com.ververica.showcase.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents a single order event consumed from the Kafka topic {@code orders}.
 * This is a plain POJO — Flink's PojoSerializer handles it without Kryo fallback
 * as long as all fields are public or have getters/setters and there is a no-arg constructor.
 */
public class Order implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("orderId")
    private String orderId;

    @JsonProperty("customerId")
    private String customerId;

    @JsonProperty("productId")
    private String productId;

    @JsonProperty("productName")
    private String productName;

    /** One of: ELECTRONICS, CLOTHING, FOOD, HOME, SPORTS, BOOKS */
    @JsonProperty("category")
    private String category;

    /** One of: NORTH, SOUTH, EAST, WEST, CENTRAL */
    @JsonProperty("region")
    private String region;

    @JsonProperty("quantity")
    private int quantity;

    @JsonProperty("unitPrice")
    private double unitPrice;

    /** Pre-computed as quantity * unitPrice by the producer. */
    @JsonProperty("totalAmount")
    private double totalAmount;

    /** Event time: epoch milliseconds from the order creation timestamp. */
    @JsonProperty("orderTimestamp")
    private long orderTimestamp;

    /** Processing time: epoch milliseconds set at ingestion into Kafka. */
    @JsonProperty("ingestionTimestamp")
    private long ingestionTimestamp;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Required by Flink's PojoSerializer and Jackson. */
    public Order() {}

    public Order(
            String orderId,
            String customerId,
            String productId,
            String productName,
            String category,
            String region,
            int quantity,
            double unitPrice,
            double totalAmount,
            long orderTimestamp,
            long ingestionTimestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.region = region;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.totalAmount = totalAmount;
        this.orderTimestamp = orderTimestamp;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    // -------------------------------------------------------------------------
    // Getters and setters — required for Flink POJO detection
    // -------------------------------------------------------------------------

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getUnitPrice() { return unitPrice; }
    public void setUnitPrice(double unitPrice) { this.unitPrice = unitPrice; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    public long getOrderTimestamp() { return orderTimestamp; }
    public void setOrderTimestamp(long orderTimestamp) { this.orderTimestamp = orderTimestamp; }

    public long getIngestionTimestamp() { return ingestionTimestamp; }
    public void setIngestionTimestamp(long ingestionTimestamp) { this.ingestionTimestamp = ingestionTimestamp; }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", category='" + category + '\'' +
                ", region='" + region + '\'' +
                ", totalAmount=" + totalAmount +
                ", orderTimestamp=" + orderTimestamp +
                '}';
    }
}
