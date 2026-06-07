package com.iot.model;

import java.io.Serializable;

/**
 * Обогащённое событие IoT устройства после join со справочником device_types.
 * Содержит название типа устройства.
 */
public class EnrichedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private int deviceTypeId;
    private String typeName;        
    private long timestamp;         
    private double temperature;
    private int humidity;

    // Конструктор по умолчанию
    public EnrichedEvent() {}

    public EnrichedEvent(int deviceTypeId, String typeName, long timestamp, double temperature, int humidity) {
        this.deviceTypeId = deviceTypeId;
        this.typeName = typeName;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    // Геттеры и сеттеры
    public int getDeviceTypeId() {
        return deviceTypeId;
    }

    public void setDeviceTypeId(int deviceTypeId) {
        this.deviceTypeId = deviceTypeId;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return "EnrichedEvent{" +
                "deviceTypeId=" + deviceTypeId +
                ", typeName='" + typeName + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                '}';
    }
}
