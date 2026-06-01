package com.ververica.showcase.producer;

import com.ververica.showcase.producer.api.SimulationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrates load-peak simulations for the VVP3 Autopilot demo.
 *
 * A peak follows this lifecycle:
 *   1. Ramp-up:   linearly increase rate from baseThroughput to baseThroughput * peakMultiplier
 *                 over rampUpSeconds using 1-second ticks.
 *   2. Sustain:   hold peak rate for peakDurationSeconds.
 *   3. Ramp-down: linearly decrease rate back to baseThroughput over rampUpSeconds.
 *
 * All state mutations that affect the rate go through {@link ThroughputController#setTargetRate},
 * keeping the two classes properly decoupled.
 */
@Component
public class PeakSimulator {

    private static final Logger log = LoggerFactory.getLogger(PeakSimulator.class);

    private final ThroughputController throughputController;
    private final KafkaProducerService.MessageCounters counters;
    private final ScheduledExecutorService scheduler;

    // Configuration — mutable at runtime via /api/config
    private volatile int baseThroughput;
    private volatile int peakMultiplier;
    private volatile int peakDurationSeconds;
    private volatile int rampUpSeconds;

    private volatile boolean peakActive = false;

    // Track in-flight ramp tasks so we can cancel them on stopPeak()
    private volatile ScheduledFuture<?> rampTask;
    private volatile ScheduledFuture<?> stopTask;

    public PeakSimulator(
            ThroughputController throughputController,
            KafkaProducerService.MessageCounters counters,
            @Value("${producer.base-throughput:500}")    int baseThroughput,
            @Value("${producer.peak-multiplier:10}")     int peakMultiplier,
            @Value("${producer.peak-duration-seconds:300}") int peakDurationSeconds,
            @Value("${producer.ramp-up-seconds:30}")     int rampUpSeconds) {
        this.throughputController  = throughputController;
        this.counters              = counters;
        this.baseThroughput        = baseThroughput;
        this.peakMultiplier        = peakMultiplier;
        this.peakDurationSeconds   = peakDurationSeconds;
        this.rampUpSeconds         = rampUpSeconds;
        // Single-threaded scheduler is sufficient: tick tasks are lightweight rate updates
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "peak-simulator-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts a peak simulation if one is not already running.
     *
     * @throws IllegalStateException if a peak is already active (caller converts to 409 Conflict)
     */
    public synchronized void startPeak() {
        if (peakActive) {
            throw new IllegalStateException("Peak simulation is already active");
        }
        peakActive = true;
        int targetPeakRate = baseThroughput * peakMultiplier;
        log.info("Starting peak: ramping from {} to {} msg/s over {}s, sustaining {}s",
                 baseThroughput, targetPeakRate, rampUpSeconds, peakDurationSeconds);

        scheduleRamp(baseThroughput, targetPeakRate, rampUpSeconds, () -> {
            log.info("Peak reached {} msg/s — sustaining for {}s", targetPeakRate, peakDurationSeconds);
            // After sustain period, automatically ramp back down
            stopTask = scheduler.schedule(this::doStopPeak, peakDurationSeconds, TimeUnit.SECONDS);
        });
    }

    /**
     * Immediately initiates a ramp-down to baseThroughput, cancelling any pending stop task.
     */
    public synchronized void stopPeak() {
        if (!peakActive) {
            log.info("stopPeak called but no peak is active — ignoring");
            return;
        }
        doStopPeak();
    }

    /** Internal stop logic — safe to call from scheduler thread or REST thread. */
    private synchronized void doStopPeak() {
        // Cancel pending auto-stop if it was scheduled
        if (stopTask != null && !stopTask.isDone()) {
            stopTask.cancel(false);
        }
        // Cancel any in-flight ramp (e.g., still ramping up when manual stop is called)
        if (rampTask != null && !rampTask.isDone()) {
            rampTask.cancel(false);
        }

        int currentRate = throughputController.getTargetRate();
        log.info("Stopping peak: ramping from {} back to {} msg/s over {}s",
                 currentRate, baseThroughput, rampUpSeconds);

        scheduleRamp(currentRate, baseThroughput, rampUpSeconds, () -> {
            peakActive = false;
            log.info("Peak simulation ended — rate restored to {} msg/s", baseThroughput);
        });
    }

    /**
     * Schedules a linear rate ramp from {@code fromRate} to {@code toRate} over
     * {@code durationSeconds} using 1-second ticks.
     *
     * @param fromRate        starting rate (msg/s)
     * @param toRate          ending rate (msg/s)
     * @param durationSeconds total ramp duration
     * @param onComplete      callback executed after the final tick
     */
    private void scheduleRamp(int fromRate, int toRate, int durationSeconds, Runnable onComplete) {
        if (durationSeconds <= 0) {
            // Instant transition — no ramp needed
            throughputController.setTargetRate(toRate);
            onComplete.run();
            return;
        }

        double stepSize = (double)(toRate - fromRate) / durationSeconds;
        int[] tick = {0}; // mutable counter captured in lambda

        rampTask = scheduler.scheduleAtFixedRate(() -> {
            tick[0]++;
            int newRate;
            if (tick[0] >= durationSeconds) {
                newRate = toRate;
            } else {
                newRate = (int) Math.round(fromRate + stepSize * tick[0]);
            }
            throughputController.setTargetRate(newRate);
            log.debug("Ramp tick {}/{}: rate={} msg/s", tick[0], durationSeconds, newRate);

            if (tick[0] >= durationSeconds) {
                rampTask.cancel(false);   // stop further ticks
                onComplete.run();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Returns a snapshot of the current simulation state.
     * Called by the REST layer to serve GET /api/status.
     */
    public SimulationStatus getStatus() {
        return new SimulationStatus(
            throughputController.getTargetRate(),
            baseThroughput,
            peakActive,
            peakMultiplier,
            peakDurationSeconds,
            rampUpSeconds,
            counters.getMessagesProduced(),
            counters.getErrorCount()
        );
    }

    // --- Config update methods called by SimulationController /api/config ---

    public synchronized void updateConfig(int newBase, int newMultiplier,
                                          int newDurationSeconds, int newRampUpSeconds) {
        if (peakActive) {
            log.warn("Config updated while peak is active — new values take effect on next peak");
        }
        this.baseThroughput      = newBase;
        this.peakMultiplier      = newMultiplier;
        this.peakDurationSeconds = newDurationSeconds;
        this.rampUpSeconds       = newRampUpSeconds;
        // Immediately apply new base rate if no peak is running
        if (!peakActive) {
            throughputController.setTargetRate(newBase);
        }
        log.info("Config updated: base={} multiplier={} duration={}s rampUp={}s",
                 newBase, newMultiplier, newDurationSeconds, newRampUpSeconds);
    }

    public int getBaseThroughput()      { return baseThroughput; }
    public int getPeakMultiplier()      { return peakMultiplier; }
    public int getPeakDurationSeconds() { return peakDurationSeconds; }
    public int getRampUpSeconds()       { return rampUpSeconds; }
    public boolean isPeakActive()       { return peakActive; }

    @PreDestroy
    public void shutdown() {
        log.info("PeakSimulator shutting down");
        scheduler.shutdownNow();
    }
}
