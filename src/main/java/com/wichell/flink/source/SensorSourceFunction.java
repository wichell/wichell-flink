package com.wichell.flink.source;

import com.wichell.flink.model.SensorReading;
import lombok.Builder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 传感器数据模拟源
 *
 * 用于演示和测试场景，生成模拟的传感器数据流
 *
 * 特性：
 * - 支持自定义传感器 ID 列表
 * - 可配置数据生成间隔
 * - 可配置温度和湿度的基准值及波动范围
 * - 自动生成位置信息
 *
 * 使用示例：
 * <pre>
 * // 使用默认配置
 * env.addSource(SensorSourceFunction.builder().build());
 *
 * // 自定义配置
 * env.addSource(SensorSourceFunction.builder()
 *     .sensorIds(new String[]{"sensor_A", "sensor_B"})
 *     .intervalMillis(2000L)
 *     .baseTemperature(25.0)
 *     .temperatureRange(20.0)
 *     .build());
 * </pre>
 *
 * @author wichell
 */
@Builder
public class SensorSourceFunction implements SourceFunction<SensorReading> {

    /**
     * 传感器 ID 列表，默认为 sensor_1, sensor_2, sensor_3
     */
    @Builder.Default
    private String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};

    /**
     * 数据生成间隔（毫秒），默认 1000ms
     */
    @Builder.Default
    private long intervalMillis = 1000L;

    /**
     * 温度基准值（摄氏度），默认 20.0
     */
    @Builder.Default
    private double baseTemperature = 20.0;

    /**
     * 温度波动范围（摄氏度），默认 30.0
     * 实际温度 = baseTemperature + random * temperatureRange
     */
    @Builder.Default
    private double temperatureRange = 30.0;

    /**
     * 湿度基准值（百分比），默认 40.0
     */
    @Builder.Default
    private double baseHumidity = 40.0;

    /**
     * 湿度波动范围（百分比），默认 40.0
     * 实际湿度 = baseHumidity + random * humidityRange
     */
    @Builder.Default
    private double humidityRange = 40.0;

    /**
     * 运行标志
     */
    @Builder.Default
    private volatile boolean running = true;

    /**
     * 随机数生成器
     */
    private final Random random = new Random();

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        while (running) {
            for (String sensorId : sensorIds) {
                SensorReading reading = SensorReading.builder()
                        .sensorId(sensorId)
                        .timestamp(System.currentTimeMillis())
                        .temperature(baseTemperature + random.nextDouble() * temperatureRange)
                        .humidity(baseHumidity + random.nextDouble() * humidityRange)
                        .location("room_" + sensorId.split("_")[sensorId.split("_").length - 1])
                        .build();

                ctx.collect(reading);
            }
            Thread.sleep(intervalMillis);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
