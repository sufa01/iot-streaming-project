package com.iot.model;

import java.io.Serializable;

/**
 * Результат агрегации по минутному окну:
 * - Время окончания окна (форматируется как HH:MM при выводе)
 * - Тип устройства (из справочника PostgreSQL)
 * - Средняя температура
 * - Медиана влажности
 */
public class AggregateResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private int deviceTypeId;
    private String typeName;
    private long windowEnd;          
    private double avgTemperature;
    private double medianHumidity;

    // Конструктор по умолчанию (для Flink)
    public AggregateResult() {}

    public AggregateResult(int deviceTypeId, String typeName, long windowEnd, 
                          double avgTemperature, double medianHumidity) {
        this.deviceTypeId = deviceTypeId;
        this.typeName = typeName;
        this.windowEnd = windowEnd;
        this.avgTemperature = avgTemperature;
        this.medianHumidity = medianHumidity;
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

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    public double getMedianHumidity() {
        return medianHumidity;
    }

    public void setMedianHumidity(double medianHumidity) {
        this.medianHumidity = medianHumidity;
    }

    @Override
    public String toString() {
        return "AggregateResult{" +
                "deviceTypeId=" + deviceTypeId +
                ", typeName='" + typeName + '\'' +
                ", windowEnd=" + windowEnd +
                ", avgTemperature=" + avgTemperature +
                ", medianHumidity=" + medianHumidity +
                '}';
    }
}
