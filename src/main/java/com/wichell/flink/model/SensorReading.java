package com.wichell.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 传感器数据模型
 *
 * 用于演示流处理场景中常见的传感器/设备数据处理
 * 实现 Serializable 接口是 Flink 序列化的基本要求
 *
 * @author wichell
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 传感器 ID - 用作分区键（KeyBy）
     */
    private String sensorId;

    /**
     * 时间戳 - 事件时间（Event Time）
     * 用于基于事件时间的窗口操作
     */
    private Long timestamp;

    /**
     * 温度值 - 传感器采集的温度数据
     */
    private Double temperature;

    /**
     * 湿度值 - 传感器采集的湿度数据
     */
    private Double humidity;

    /**
     * 数据来源/位置
     */
    private String location;

    /**
     * 创建测试数据的便捷方法
     */
    public static SensorReading of(String sensorId, Double temperature) {
        return SensorReading.builder()
                .sensorId(sensorId)
                .timestamp(System.currentTimeMillis())
                .temperature(temperature)
                .humidity(50.0)
                .location("default")
                .build();
    }
}
