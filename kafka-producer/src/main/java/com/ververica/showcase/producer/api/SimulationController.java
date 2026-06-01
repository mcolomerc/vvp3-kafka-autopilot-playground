package com.ververica.showcase.producer.api;

import com.ververica.showcase.producer.PeakSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST API for controlling the throughput simulation.
 *
 * This is the primary operator interface used during VVP3 Autopilot demos to
 * trigger load peaks that drive Flink autoscaler scale-up/down events.
 *
 * Endpoints:
 *   GET  /api/status        — current simulation state + counters
 *   POST /api/config        — update base throughput, peak shape
 *   POST /api/peak/start    — begin a peak simulation
 *   POST /api/peak/stop     — abort a peak and ramp back to base
 */
@RestController
@RequestMapping("/api")
public class SimulationController {

    private static final Logger log = LoggerFactory.getLogger(SimulationController.class);

    private final PeakSimulator peakSimulator;

    public SimulationController(PeakSimulator peakSimulator) {
        this.peakSimulator = peakSimulator;
    }

    /**
     * Returns the current simulation status including throughput, counters, and config.
     */
    @GetMapping("/status")
    public ResponseEntity<SimulationStatus> getStatus() {
        return ResponseEntity.ok(peakSimulator.getStatus());
    }

    /**
     * Updates simulation configuration.
     *
     * Accepted body fields (all optional — missing fields keep their current value):
     * <pre>
     * {
     *   "baseThroughput":     500,
     *   "peakMultiplier":     10,
     *   "peakDurationSeconds": 300,
     *   "rampUpSeconds":      30
     * }
     * </pre>
     *
     * If no peak is currently active, the base throughput is applied immediately.
     */
    @PostMapping("/config")
    public ResponseEntity<SimulationStatus> updateConfig(@RequestBody Map<String, Integer> body) {
        int newBase     = body.getOrDefault("baseThroughput",     peakSimulator.getBaseThroughput());
        int newMult     = body.getOrDefault("peakMultiplier",     peakSimulator.getPeakMultiplier());
        int newDuration = body.getOrDefault("peakDurationSeconds",peakSimulator.getPeakDurationSeconds());
        int newRamp     = body.getOrDefault("rampUpSeconds",      peakSimulator.getRampUpSeconds());

        if (newBase < 1 || newMult < 1 || newDuration < 1 || newRamp < 0) {
            log.warn("Invalid config request: base={} mult={} duration={} ramp={}",
                     newBase, newMult, newDuration, newRamp);
            return ResponseEntity.badRequest().build();
        }

        peakSimulator.updateConfig(newBase, newMult, newDuration, newRamp);
        log.info("Config updated via REST: base={} multiplier={} duration={}s rampUp={}s",
                 newBase, newMult, newDuration, newRamp);
        return ResponseEntity.ok(peakSimulator.getStatus());
    }

    /**
     * Starts a peak simulation.
     * Returns 409 Conflict if a peak is already running.
     */
    @PostMapping("/peak/start")
    public ResponseEntity<SimulationStatus> startPeak() {
        try {
            peakSimulator.startPeak();
            log.info("Peak simulation started via REST");
            return ResponseEntity.ok(peakSimulator.getStatus());
        } catch (IllegalStateException e) {
            log.warn("Peak start rejected: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    /**
     * Stops the current peak simulation and ramps back to base throughput.
     * Idempotent: returns 200 even if no peak is active.
     */
    @PostMapping("/peak/stop")
    public ResponseEntity<SimulationStatus> stopPeak() {
        peakSimulator.stopPeak();
        log.info("Peak simulation stopped via REST");
        return ResponseEntity.ok(peakSimulator.getStatus());
    }
}
