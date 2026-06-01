package com.ververica.showcase.producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an order event produced to the Kafka "orders" topic.
 * Field names use camelCase and map directly to the JSON schema consumed
 * by the downstream Flink pipeline.
 */
public class Order {

    @JsonProperty("orderId")
    private String orderId;

    @JsonProperty("customerId")
    private String customerId;

    @JsonProperty("productId")
    private String productId;

    @JsonProperty("productName")
    private String productName;

    @JsonProperty("category")
    private String category;

    @JsonProperty("region")
    private String region;

    @JsonProperty("quantity")
    private int quantity;

    @JsonProperty("unitPrice")
    private double unitPrice;

    @JsonProperty("totalAmount")
    private double totalAmount;

    @JsonProperty("orderTimestamp")
    private long orderTimestamp;

    @JsonProperty("ingestionTimestamp")
    private long ingestionTimestamp;

    // No-arg constructor required by Jackson
    public Order() {
    }

    public Order(String orderId, String customerId, String productId, String productName,
                 String category, String region, int quantity, double unitPrice,
                 double totalAmount, long orderTimestamp, long ingestionTimestamp) {
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
        return "Order{orderId='" + orderId + "', category='" + category +
               "', region='" + region + "', totalAmount=" + totalAmount + "}";
    }
}
