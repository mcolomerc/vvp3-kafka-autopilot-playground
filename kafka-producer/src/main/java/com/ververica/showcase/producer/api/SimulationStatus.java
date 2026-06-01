package com.ververica.showcase.producer.api;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO returned by the REST API to describe the current simulation state.
 * Consumed by monitoring dashboards and the peak-trigger scripts.
 */
public class SimulationStatus {

    @JsonProperty("currentThroughput")
    private int currentThroughput;

    @JsonProperty("baseThroughput")
    private int baseThroughput;

    @JsonProperty("peakActive")
    private boolean peakActive;

    @JsonProperty("peakMultiplier")
    private int peakMultiplier;

    @JsonProperty("peakDurationSeconds")
    private int peakDurationSeconds;

    @JsonProperty("rampUpSeconds")
    private int rampUpSeconds;

    @JsonProperty("messagesProduced")
    private long messagesProduced;

    @JsonProperty("errorCount")
    private long errorCount;

    public SimulationStatus() {
    }

    public SimulationStatus(int currentThroughput, int baseThroughput, boolean peakActive,
                            int peakMultiplier, int peakDurationSeconds, int rampUpSeconds,
                            long messagesProduced, long errorCount) {
        this.currentThroughput = currentThroughput;
        this.baseThroughput = baseThroughput;
        this.peakActive = peakActive;
        this.peakMultiplier = peakMultiplier;
        this.peakDurationSeconds = peakDurationSeconds;
        this.rampUpSeconds = rampUpSeconds;
        this.messagesProduced = messagesProduced;
        this.errorCount = errorCount;
    }

    public int getCurrentThroughput() { return currentThroughput; }
    public void setCurrentThroughput(int currentThroughput) { this.currentThroughput = currentThroughput; }

    public int getBaseThroughput() { return baseThroughput; }
    public void setBaseThroughput(int baseThroughput) { this.baseThroughput = baseThroughput; }

    public boolean isPeakActive() { return peakActive; }
    public void setPeakActive(boolean peakActive) { this.peakActive = peakActive; }

    public int getPeakMultiplier() { return peakMultiplier; }
    public void setPeakMultiplier(int peakMultiplier) { this.peakMultiplier = peakMultiplier; }

    public int getPeakDurationSeconds() { return peakDurationSeconds; }
    public void setPeakDurationSeconds(int peakDurationSeconds) { this.peakDurationSeconds = peakDurationSeconds; }

    public int getRampUpSeconds() { return rampUpSeconds; }
    public void setRampUpSeconds(int rampUpSeconds) { this.rampUpSeconds = rampUpSeconds; }

    public long getMessagesProduced() { return messagesProduced; }
    public void setMessagesProduced(long messagesProduced) { this.messagesProduced = messagesProduced; }

    public long getErrorCount() { return errorCount; }
    public void setErrorCount(long errorCount) { this.errorCount = errorCount; }
}
