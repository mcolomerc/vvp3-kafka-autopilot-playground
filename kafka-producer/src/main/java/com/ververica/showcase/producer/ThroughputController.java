package com.ververica.showcase.producer;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Controls the rate at which the producer sends messages to Kafka.
 *
 * Wraps Guava's {@link RateLimiter} (token-bucket algorithm) which provides
 * smooth, bursty-safe throughput limiting. The rate can be changed at runtime
 * by the {@link PeakSimulator} during ramp-up/ramp-down sequences.
 *
 * Thread-safety: {@code setTargetRate} is called from the ScheduledExecutorService
 * inside PeakSimulator, while {@code acquire} is called from the producer thread.
 * Guava's RateLimiter.setRate() is thread-safe, and the volatile field ensures
 * visibility of the updated rate across threads.
 */
@Component
public class ThroughputController {

    private static final Logger log = LoggerFactory.getLogger(ThroughputController.class);

    private final RateLimiter rateLimiter;

    // volatile so changes from the scheduler thread are visible to the producer thread
    private volatile int targetRate;

    public ThroughputController(
            @Value("${producer.base-throughput:500}") int baseThroughput) {
        this.targetRate = baseThroughput;
        // RateLimiter.create uses a SmoothBursty policy; warmupPeriod variant is
        // NOT used here because PeakSimulator handles gradual ramp-up externally.
        this.rateLimiter = RateLimiter.create(baseThroughput);
        log.info("ThroughputController initialised at {} msg/s", baseThroughput);
    }

    /**
     * Updates the RateLimiter to the new rate and records it.
     * Called by PeakSimulator on each ramp tick (every 1 second).
     *
     * @param msgsPerSec desired messages per second; must be >= 1
     */
    public void setTargetRate(int msgsPerSec) {
        int clamped = Math.max(1, msgsPerSec);
        this.targetRate = clamped;
        rateLimiter.setRate(clamped);
        log.debug("Rate updated to {} msg/s", clamped);
    }

    /**
     * Blocks until the RateLimiter grants a permit.
     * Called by the producer loop for every message — this is the primary
     * back-pressure mechanism keeping throughput at the configured rate.
     */
    public void acquire() {
        rateLimiter.acquire();
    }

    /** Returns the currently configured target rate (msg/s). */
    public int getTargetRate() {
        return targetRate;
    }
}
