package com.checkpoint.iot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

/**
 * A single temperature measurement from an IoT device.
 *
 * <p>Plain POJO (public no-arg constructor, standard getters/setters) so Flink's
 * type extraction and serialization work without extra configuration, and so
 * Jackson can deserialize it straight from the JSON input file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Reading {

    private String deviceId;
    private double temperatureC;
    private long timestampMillis;

    public Reading() {
        // Required by Flink's POJO serializer and Jackson.
    }

    public Reading(String deviceId, double temperatureC, long timestampMillis) {
        this.deviceId = deviceId;
        this.temperatureC = temperatureC;
        this.timestampMillis = timestampMillis;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public double getTemperatureC() {
        return temperatureC;
    }

    public void setTemperatureC(double temperatureC) {
        this.temperatureC = temperatureC;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public void setTimestampMillis(long timestampMillis) {
        this.timestampMillis = timestampMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Reading reading = (Reading) o;
        return Double.compare(reading.temperatureC, temperatureC) == 0
                && timestampMillis == reading.timestampMillis
                && Objects.equals(deviceId, reading.deviceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, temperatureC, timestampMillis);
    }

    @Override
    public String toString() {
        return "Reading{deviceId='" + deviceId + '\''
                + ", temperatureC=" + temperatureC
                + ", timestampMillis=" + timestampMillis + '}';
    }
}
