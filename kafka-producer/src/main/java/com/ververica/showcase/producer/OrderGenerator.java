package com.ververica.showcase.producer;

import com.ververica.showcase.producer.model.Order;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Generates statistically realistic fake Order events.
 *
 * Category weights mirror real retail distributions:
 *   ELECTRONICS 30%, CLOTHING 20%, FOOD 20%, HOME 15%, SPORTS 10%, BOOKS 5%
 *
 * Price ranges are category-specific to produce meaningful revenue aggregations
 * in the downstream Flink windowing jobs.
 */
@Component
public class OrderGenerator {

    private static final String[] REGIONS = {"NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"};

    private static final String[] CATEGORIES = {
        "ELECTRONICS", "ELECTRONICS", "ELECTRONICS",   // 30 %
        "CLOTHING",    "CLOTHING",                     // 20 %
        "FOOD",        "FOOD",                         // 20 %
        "HOME",        "HOME", "HOME",                 // 15 % (rounded up — 3 slots)
        "SPORTS",                                      // 10 %
        "BOOKS"                                        // 5 %  (1 slot ≈ 8.3%; close enough for demo)
    };
    // Effective weights after rounding: ELECTRONICS 3/12, CLOTHING 2/12, FOOD 2/12,
    // HOME 3/12, SPORTS 1/12, BOOKS 1/12. To match spec exactly we use a cumulative
    // probability table instead.
    //  0.00–0.30 → ELECTRONICS
    //  0.30–0.50 → CLOTHING
    //  0.50–0.70 → FOOD
    //  0.70–0.85 → HOME
    //  0.85–0.95 → SPORTS
    //  0.95–1.00 → BOOKS

    private static final Map<String, double[]> PRICE_RANGES;
    private static final Map<String, List<String>> PRODUCT_NAMES;

    static {
        PRICE_RANGES = new HashMap<>();
        PRICE_RANGES.put("ELECTRONICS", new double[]{50.0,  2000.0});
        PRICE_RANGES.put("CLOTHING",    new double[]{20.0,   500.0});
        PRICE_RANGES.put("FOOD",        new double[]{ 5.0,    50.0});
        PRICE_RANGES.put("HOME",        new double[]{30.0,  1000.0});
        PRICE_RANGES.put("SPORTS",      new double[]{20.0,   800.0});
        PRICE_RANGES.put("BOOKS",       new double[]{ 8.0,    80.0});

        PRODUCT_NAMES = new HashMap<>();
        PRODUCT_NAMES.put("ELECTRONICS", Arrays.asList(
            "Wireless Headphones", "4K Smart TV", "Gaming Laptop", "Bluetooth Speaker",
            "Noise-Cancelling Earbuds", "Mechanical Keyboard", "USB-C Hub", "Smart Watch",
            "Portable Charger", "Webcam HD"
        ));
        PRODUCT_NAMES.put("CLOTHING", Arrays.asList(
            "Running Shoes", "Denim Jacket", "Cotton T-Shirt", "Wool Sweater",
            "Yoga Pants", "Hiking Boots", "Waterproof Jacket", "Polo Shirt",
            "Casual Sneakers", "Winter Coat"
        ));
        PRODUCT_NAMES.put("FOOD", Arrays.asList(
            "Organic Coffee Beans", "Extra Virgin Olive Oil", "Dark Chocolate Bar",
            "Protein Powder", "Herbal Tea Set", "Dried Fruit Mix", "Almond Butter",
            "Sparkling Water Pack", "Granola Bars", "Hot Sauce Collection"
        ));
        PRODUCT_NAMES.put("HOME", Arrays.asList(
            "Stand Mixer", "Air Purifier", "Robot Vacuum", "Coffee Maker",
            "Memory Foam Pillow", "Bamboo Cutting Board", "Stainless Steel Cookware Set",
            "Electric Kettle", "Smart Thermostat", "LED Desk Lamp"
        ));
        PRODUCT_NAMES.put("SPORTS", Arrays.asList(
            "Yoga Mat", "Adjustable Dumbbells", "Resistance Bands Set", "Foam Roller",
            "Jump Rope", "Pull-Up Bar", "Cycling Helmet", "Tennis Racket",
            "Swimming Goggles", "Running Belt"
        ));
        PRODUCT_NAMES.put("BOOKS", Arrays.asList(
            "Clean Code", "Designing Data-Intensive Applications", "The Pragmatic Programmer",
            "Domain-Driven Design", "Site Reliability Engineering",
            "Kafka: The Definitive Guide", "Learning Apache Flink", "Java Concurrency in Practice",
            "The DevOps Handbook", "Building Microservices"
        ));
    }

    private final Random random = new Random();

    /**
     * Generates a single random Order.
     * Thread-safe: {@link Random} is used exclusively from the single producer thread.
     */
    public Order generate() {
        String category = pickCategory();
        String region   = REGIONS[random.nextInt(REGIONS.length)];
        int quantity    = 1 + random.nextInt(10);

        double[] priceRange = PRICE_RANGES.get(category);
        // Round to 2 decimal places for realistic pricing
        double unitPrice = Math.round((priceRange[0] + random.nextDouble() * (priceRange[1] - priceRange[0])) * 100.0) / 100.0;
        double totalAmount = Math.round(quantity * unitPrice * 100.0) / 100.0;

        List<String> names = PRODUCT_NAMES.get(category);
        String productName = names.get(random.nextInt(names.size()));

        long now = System.currentTimeMillis();

        return new Order(
            UUID.randomUUID().toString(),
            "CUST-" + String.format("%05d", random.nextInt(10000)),
            "PROD-" + String.format("%05d", random.nextInt(1000)),
            productName,
            category,
            region,
            quantity,
            unitPrice,
            totalAmount,
            now,
            now   // ingestionTimestamp set at point of creation; producer will not re-set it
        );
    }

    /** Weighted category selection using cumulative probability table. */
    private String pickCategory() {
        double r = random.nextDouble();
        if (r < 0.30) return "ELECTRONICS";
        if (r < 0.50) return "CLOTHING";
        if (r < 0.70) return "FOOD";
        if (r < 0.85) return "HOME";
        if (r < 0.95) return "SPORTS";
        return "BOOKS";
    }
}
