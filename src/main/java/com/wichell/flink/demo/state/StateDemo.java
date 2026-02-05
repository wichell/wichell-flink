package com.wichell.flink.demo.state;

import com.wichell.flink.model.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

/**
 * Flink 状态管理详细演示
 *
 * 状态（State）是 Flink 流处理的核心概念之一，用于存储中间计算结果。
 *
 * ==================== 状态类型 ====================
 *
 * 1. 键控状态（Keyed State）- 最常用
 *    - 与特定的 key 绑定
 *    - 只能在 KeyedStream 上使用
 *    - 类型：ValueState, ListState, MapState, ReducingState, AggregatingState
 *
 * 2. 算子状态（Operator State）
 *    - 与算子实例绑定
 *    - 通常用于 Source/Sink
 *    - 类型：ListState, UnionListState, BroadcastState
 *
 * ==================== 状态后端 ====================
 *
 * 1. HashMapStateBackend（默认）
 *    - 状态存储在 JVM 堆内存
 *    - 适用于小状态场景
 *    - 检查点时进行完整快照
 *
 * 2. EmbeddedRocksDBStateBackend
 *    - 状态存储在 RocksDB（本地磁盘）
 *    - 适用于大状态场景
 *    - 支持增量检查点
 *
 * ==================== 状态 TTL ====================
 *
 * 可以为状态设置过期时间（TTL），自动清理过期状态。
 *
 * @author wichell
 */
@Component
public class StateDemo {

    /**
     * 演示 ValueState 的使用
     *
     * ValueState 是最简单的状态类型：
     * - 存储单个值
     * - 每个 key 对应一个 ValueState
     *
     * 使用场景：
     * - 存储上一次的值（用于比较）
     * - 存储累计结果
     * - 存储配置信息
     */
    public void demonstrateValueState(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== ValueState 演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 使用 ValueState 检测温度突变 ====================
        /*
         * 场景：检测传感器温度变化是否超过阈值
         * 如果相邻两次温度差超过 10 度，则告警
         */
        DataStream<String> alertStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new RichFlatMapFunction<SensorReading, String>() {

                    // 声明 ValueState，存储上一次的温度
                    private ValueState<Double> lastTemperatureState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态
                        // ValueStateDescriptor 包含状态名称和类型信息
                        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                                "last-temperature",  // 状态名称（在检查点中标识）
                                Double.class          // 状态值类型
                        );

                        // 从运行时上下文获取状态
                        lastTemperatureState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void flatMap(SensorReading reading, Collector<String> out) throws Exception {
                        // 获取上一次的温度
                        Double lastTemp = lastTemperatureState.value();

                        // 如果不是第一次，则比较温度差
                        if (lastTemp != null) {
                            double diff = Math.abs(reading.getTemperature() - lastTemp);
                            if (diff > 10) {
                                out.collect(String.format(
                                        "⚠️ 温度突变告警: %s, 上次: %.2f°C, 当前: %.2f°C, 变化: %.2f°C",
                                        reading.getSensorId(), lastTemp, reading.getTemperature(), diff
                                ));
                            }
                        }

                        // 更新状态为当前温度
                        lastTemperatureState.update(reading.getTemperature());
                    }
                });

        alertStream.print("温度突变告警");
    }

    /**
     * 演示 ListState 的使用
     *
     * ListState 存储一个列表：
     * - 可以添加多个元素
     * - 支持迭代访问
     * - 适用于需要存储历史记录的场景
     *
     * 使用场景：
     * - 存储最近 N 条记录
     * - 存储待处理的事件列表
     * - 实现自定义窗口
     */
    public void demonstrateListState(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== ListState 演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 使用 ListState 计算移动平均 ====================
        /*
         * 场景：计算每个传感器最近 5 次温度的移动平均值
         */
        DataStream<String> movingAvgStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new RichFlatMapFunction<SensorReading, String>() {

                    // 存储最近的温度记录
                    private ListState<Double> recentTemperatures;
                    private static final int WINDOW_SIZE = 5;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Double> descriptor = new ListStateDescriptor<>(
                                "recent-temps",
                                Double.class
                        );
                        recentTemperatures = getRuntimeContext().getListState(descriptor);
                    }

                    @Override
                    public void flatMap(SensorReading reading, Collector<String> out) throws Exception {
                        // 添加新温度
                        recentTemperatures.add(reading.getTemperature());

                        // 获取所有温度并转换为列表
                        List<Double> temps = new ArrayList<>();
                        for (Double temp : recentTemperatures.get()) {
                            temps.add(temp);
                        }

                        // 如果超过窗口大小，移除最旧的
                        if (temps.size() > WINDOW_SIZE) {
                            temps = temps.subList(temps.size() - WINDOW_SIZE, temps.size());
                            // 更新状态
                            recentTemperatures.update(temps);
                        }

                        // 计算平均值
                        double avg = temps.stream()
                                .mapToDouble(Double::doubleValue)
                                .average()
                                .orElse(0);

                        out.collect(String.format(
                                "[移动平均] %s: 最近%d条平均温度 %.2f°C",
                                reading.getSensorId(), temps.size(), avg
                        ));
                    }
                });

        movingAvgStream.print("移动平均");
    }

    /**
     * 演示 MapState 的使用
     *
     * MapState 存储键值对：
     * - 类似于 Map<UK, UV>
     * - 可以存储多个键值对
     * - 支持单独更新某个键
     *
     * 使用场景：
     * - 存储用户的多个属性
     * - 存储分类统计结果
     * - 实现去重逻辑
     */
    public void demonstrateMapState(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== MapState 演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 使用 MapState 统计各位置的温度情况 ====================
        /*
         * 场景：按传感器分组，统计各个位置的最高/最低温度
         */
        DataStream<String> locationStatsStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new RichFlatMapFunction<SensorReading, String>() {

                    // MapState：位置 -> (最高温度, 最低温度)
                    private MapState<String, Tuple2<Double, Double>> locationStats;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Tuple2<Double, Double>> descriptor =
                                new MapStateDescriptor<>(
                                        "location-stats",
                                        TypeInformation.of(String.class),
                                        TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {})
                                );
                        locationStats = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void flatMap(SensorReading reading, Collector<String> out) throws Exception {
                        String location = reading.getLocation();
                        Double temp = reading.getTemperature();

                        // 获取当前位置的统计
                        Tuple2<Double, Double> stats = locationStats.get(location);

                        if (stats == null) {
                            // 第一次记录该位置
                            stats = Tuple2.of(temp, temp);
                        } else {
                            // 更新最高/最低温度
                            stats = Tuple2.of(
                                    Math.max(stats.f0, temp),
                                    Math.min(stats.f1, temp)
                            );
                        }

                        // 更新状态
                        locationStats.put(location, stats);

                        // 输出所有位置的统计
                        StringBuilder sb = new StringBuilder();
                        sb.append(String.format("[%s 位置统计] ", reading.getSensorId()));
                        for (Map.Entry<String, Tuple2<Double, Double>> entry : locationStats.entries()) {
                            sb.append(String.format("%s(最高:%.1f°C,最低:%.1f°C) ",
                                    entry.getKey(), entry.getValue().f0, entry.getValue().f1));
                        }
                        out.collect(sb.toString());
                    }
                });

        locationStatsStream.print("位置统计");
    }

    /**
     * 演示 ReducingState 的使用
     *
     * ReducingState 自动进行增量聚合：
     * - 每次添加元素时自动与之前的结果合并
     * - 只保留一个聚合结果
     * - 需要提供 ReduceFunction
     *
     * 使用场景：
     * - 累加求和
     * - 求最大/最小值
     * - 任何符合结合律的聚合操作
     */
    public void demonstrateReducingState(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== ReducingState 演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 使用 ReducingState 累计温度和 ====================
        DataStream<String> sumStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .map(new RichMapFunction<SensorReading, String>() {

                    // ReducingState 自动累加
                    private ReducingState<Double> temperatureSum;
                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建 ReducingState，提供 ReduceFunction
                        ReducingStateDescriptor<Double> sumDescriptor =
                                new ReducingStateDescriptor<>(
                                        "temp-sum",
                                        Double::sum,  // 累加函数
                                        Double.class
                                );
                        temperatureSum = getRuntimeContext().getReducingState(sumDescriptor);

                        // 计数状态
                        ValueStateDescriptor<Integer> countDescriptor =
                                new ValueStateDescriptor<>("count", Integer.class);
                        countState = getRuntimeContext().getState(countDescriptor);
                    }

                    @Override
                    public String map(SensorReading reading) throws Exception {
                        // 添加温度到 ReducingState（自动累加）
                        temperatureSum.add(reading.getTemperature());

                        // 更新计数
                        Integer count = countState.value();
                        count = (count == null) ? 1 : count + 1;
                        countState.update(count);

                        // 获取累计结果
                        Double sum = temperatureSum.get();
                        double avg = sum / count;

                        return String.format(
                                "[ReducingState] %s: 累计温度=%.2f°C, 计数=%d, 平均=%.2f°C",
                                reading.getSensorId(), sum, count, avg
                        );
                    }
                });

        sumStream.print("累计统计");
    }

    /**
     * 演示状态 TTL（Time-To-Live）
     *
     * 状态 TTL 用于自动清理过期状态：
     * - 避免状态无限增长
     * - 节省内存资源
     * - 适用于有时效性的数据
     *
     * 配置选项：
     * - 过期时间
     * - 更新策略：读取时更新 / 写入时更新
     * - 可见性：过期后是否可见
     * - 清理策略：全量快照 / 增量清理
     */
    public void demonstrateStateTTL(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 状态 TTL 演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 配置状态 TTL ====================
        DataStream<String> resultStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new RichFlatMapFunction<SensorReading, String>() {

                    private ValueState<Double> lastTempState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建 TTL 配置
                        StateTtlConfig ttlConfig = StateTtlConfig
                                // 设置过期时间为 10 秒
                                .newBuilder(Time.seconds(10))
                                // 设置更新策略：每次读取或写入都会更新过期时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                // 设置状态可见性：过期后不可见
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                // 设置清理策略
                                .cleanupFullSnapshot()  // 在全量快照时清理
                                // .cleanupIncrementally(10, true)  // 增量清理
                                // .cleanupInRocksdbCompactFilter(1000)  // RocksDB 压缩时清理
                                .build();

                        // 创建带 TTL 的状态描述符
                        ValueStateDescriptor<Double> descriptor =
                                new ValueStateDescriptor<>("last-temp-with-ttl", Double.class);
                        // 启用 TTL
                        descriptor.enableTimeToLive(ttlConfig);

                        lastTempState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void flatMap(SensorReading reading, Collector<String> out) throws Exception {
                        Double lastTemp = lastTempState.value();

                        if (lastTemp == null) {
                            out.collect(String.format(
                                    "[TTL] %s: 首次记录或状态已过期, 温度=%.2f°C",
                                    reading.getSensorId(), reading.getTemperature()
                            ));
                        } else {
                            out.collect(String.format(
                                    "[TTL] %s: 上次=%.2f°C, 当前=%.2f°C, 变化=%.2f°C",
                                    reading.getSensorId(), lastTemp, reading.getTemperature(),
                                    reading.getTemperature() - lastTemp
                            ));
                        }

                        lastTempState.update(reading.getTemperature());
                    }
                });

        resultStream.print("TTL状态");
    }

    /**
     * 演示定时器（Timer）与状态结合
     *
     * KeyedProcessFunction 提供了定时器功能：
     * - 可以注册处理时间或事件时间定时器
     * - 定时器触发时调用 onTimer 方法
     * - 常与状态结合使用
     *
     * 使用场景：
     * - 超时检测
     * - 延迟触发
     * - 定时清理
     */
    public void demonstrateTimerWithState(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 定时器与状态演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 使用定时器实现温度告警 ====================
        /*
         * 场景：如果传感器温度持续 10 秒超过 30 度，则告警
         */
        DataStream<String> alertStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {

                    // 存储上一次的温度
                    private ValueState<Double> lastTempState;
                    // 存储定时器的触发时间
                    private ValueState<Long> timerTimestampState;
                    // 告警阈值
                    private static final double THRESHOLD = 30.0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastTempState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("last-temp", Double.class));
                        timerTimestampState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("timer-ts", Long.class));
                    }

                    @Override
                    public void processElement(SensorReading reading,
                                               Context ctx,
                                               Collector<String> out) throws Exception {

                        Double lastTemp = lastTempState.value();
                        Long timerTs = timerTimestampState.value();
                        Double currentTemp = reading.getTemperature();

                        // 更新温度
                        lastTempState.update(currentTemp);

                        if (currentTemp > THRESHOLD) {
                            // 温度超过阈值
                            if (timerTs == null) {
                                // 注册 10 秒后的定时器
                                long timer = ctx.timerService().currentProcessingTime() + 10000;
                                ctx.timerService().registerProcessingTimeTimer(timer);
                                timerTimestampState.update(timer);

                                out.collect(String.format(
                                        "⚠️ %s 温度超过阈值: %.2f°C > %.2f°C，开始计时...",
                                        reading.getSensorId(), currentTemp, THRESHOLD
                                ));
                            }
                        } else {
                            // 温度恢复正常，取消定时器
                            if (timerTs != null) {
                                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                                timerTimestampState.clear();

                                out.collect(String.format(
                                        "✅ %s 温度恢复正常: %.2f°C，取消告警",
                                        reading.getSensorId(), currentTemp
                                ));
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        // 定时器触发，说明温度持续超标 10 秒
                        Double lastTemp = lastTempState.value();

                        if (lastTemp != null && lastTemp > THRESHOLD) {
                            out.collect(String.format(
                                    "🚨 严重告警: %s 温度持续超标 10 秒！当前温度: %.2f°C",
                                    ctx.getCurrentKey(), lastTemp
                            ));
                        }

                        // 清除定时器状态
                        timerTimestampState.clear();
                    }
                });

        alertStream.print("温度告警");
    }

    /**
     * 创建传感器数据源
     */
    private DataStream<SensorReading> createSensorSource(StreamExecutionEnvironment env) {
        return env.addSource(new SensorSourceFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((reading, ts) -> reading.getTimestamp())
                );
    }

    /**
     * 模拟传感器数据源
     */
    private static class SensorSourceFunction implements SourceFunction<SensorReading> {
        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            String[] sensorIds = {"sensor_1", "sensor_2"};
            String[] locations = {"room_1", "room_2", "room_3"};

            while (running) {
                for (String sensorId : sensorIds) {
                    SensorReading reading = SensorReading.builder()
                            .sensorId(sensorId)
                            .timestamp(System.currentTimeMillis())
                            // 温度在 20-40 之间波动
                            .temperature(20 + random.nextDouble() * 20)
                            .humidity(40 + random.nextDouble() * 40)
                            .location(locations[random.nextInt(locations.length)])
                            .build();

                    ctx.collect(reading);
                }
                Thread.sleep(2000);  // 每 2 秒生成一批数据
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 运行所有状态演示
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink 状态管理演示");
        System.out.println("=".repeat(60));

        // 选择一个演示运行
        demonstrateValueState(env);
        // demonstrateListState(env);
        // demonstrateMapState(env);
        // demonstrateReducingState(env);
        // demonstrateStateTTL(env);
        // demonstrateTimerWithState(env);

        env.execute("State Demo");
    }
}
