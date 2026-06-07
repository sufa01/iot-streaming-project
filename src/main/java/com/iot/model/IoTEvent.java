package com.iot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * Модель события IoT устройства.
 * Сериализуется в JSON для отправки в Kafka.
 */
public class IoTEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("deviceTypeId")
    private int deviceTypeId;

    @JsonProperty("timestamp")
    private long timestamp; // Unix timestamp в миллисекундах

    @JsonProperty("temperature")
    private double temperature;

    @JsonProperty("humidity")
    private int humidity;

    // Конструктор по умолчанию (нужен для десериализации)
    public IoTEvent() {}

    public IoTEvent(int deviceTypeId, long timestamp, double temperature, int humidity) {
        this.deviceTypeId = deviceTypeId;
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
        return "IoTEvent{" +
                "deviceTypeId=" + deviceTypeId +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                '}';
    }
}
