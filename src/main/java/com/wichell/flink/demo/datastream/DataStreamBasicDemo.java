package com.wichell.flink.demo.datastream;

import com.wichell.flink.model.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * DataStream API 核心操作演示
 *
 * DataStream API 是 Flink 流处理的核心 API，提供了丰富的转换操作：
 *
 * 1. 基础转换操作：
 *    - map: 一对一转换
 *    - flatMap: 一对多转换
 *    - filter: 过滤
 *    - keyBy: 按键分区
 *
 * 2. 聚合操作：
 *    - reduce: 滚动聚合
 *    - sum/min/max: 简单聚合
 *
 * 3. 多流操作：
 *    - union: 合并同类型流
 *    - connect: 连接不同类型流
 *    - split/select: 分流（已废弃，使用侧输出）
 *
 * 4. 物理分区操作：
 *    - shuffle: 随机分区
 *    - rebalance: 轮询分区
 *    - rescale: 本地轮询
 *    - broadcast: 广播
 *
 * @author wichell
 */
@Component
public class DataStreamBasicDemo {

    /**
     * 演示 Map 转换操作
     *
     * Map 是最基本的转换操作，将输入元素一对一转换为输出元素
     * 输入一个元素，输出一个元素（类型可以不同）
     *
     * @param env Flink 执行环境
     */
    public void demonstrateMap(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Map 操作演示 ==========");

        // 创建数据源
        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== Map 操作 ====================
        /*
         * 使用 map 提取温度值
         * 输入: SensorReading 对象
         * 输出: Double 温度值
         */
        DataStream<Double> temperatureStream = sensorStream
                .map(new MapFunction<SensorReading, Double>() {
                    @Override
                    public Double map(SensorReading reading) throws Exception {
                        // 提取温度值
                        return reading.getTemperature();
                    }
                });

        // 使用 Lambda 表达式简化（推荐）
        DataStream<Double> tempStreamLambda = sensorStream
                .map(reading -> reading.getTemperature());

        /*
         * 转换为 Tuple2 (传感器ID, 温度)
         * 便于后续的 keyBy 和聚合操作
         */
        DataStream<Tuple2<String, Double>> sensorTempStream = sensorStream
                .map(reading -> Tuple2.of(reading.getSensorId(), reading.getTemperature()))
                // 注意：使用 Lambda 时需要显式指定返回类型
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));

        temperatureStream.print("温度值");
    }

    /**
     * 演示 FlatMap 转换操作
     *
     * FlatMap 可以将一个输入元素转换为零个、一个或多个输出元素
     * 常用于：拆分、过滤+转换、生成多条记录等场景
     *
     * @param env Flink 执行环境
     */
    public void demonstrateFlatMap(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== FlatMap 操作演示 ==========");

        // 创建文本数据源
        DataStream<String> textStream = env.fromCollection(Arrays.asList(
                "hello world",
                "flink is awesome",
                "stream processing with flink"
        ));

        // ==================== FlatMap 操作 ====================
        /*
         * 经典的 WordCount 分词操作
         * 将每行文本拆分为多个单词
         */
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
                        // 按空格分割
                        String[] words = line.split("\\s+");
                        // 输出每个单词，计数为 1
                        for (String word : words) {
                            if (!word.isEmpty()) {
                                out.collect(Tuple2.of(word.toLowerCase(), 1));
                            }
                        }
                    }
                });

        // 按单词分组并求和
        DataStream<Tuple2<String, Integer>> wordCounts = wordCountStream
                .keyBy(tuple -> tuple.f0)  // 按单词分组
                .sum(1);  // 对第二个字段求和

        wordCounts.print("WordCount");
    }

    /**
     * 演示 Filter 过滤操作
     *
     * Filter 根据条件过滤数据，只保留满足条件的元素
     * 返回 true 的元素被保留，返回 false 的元素被丢弃
     *
     * @param env Flink 执行环境
     */
    public void demonstrateFilter(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Filter 操作演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== Filter 操作 ====================
        /*
         * 过滤出温度超过 30 度的数据
         * 用于告警场景
         */
        DataStream<SensorReading> highTempStream = sensorStream
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading reading) throws Exception {
                        return reading.getTemperature() > 30.0;
                    }
                });

        // 使用 Lambda 表达式
        DataStream<SensorReading> highTempLambda = sensorStream
                .filter(reading -> reading.getTemperature() > 30.0);

        // 链式调用：过滤 + 转换
        DataStream<String> alertStream = sensorStream
                .filter(r -> r.getTemperature() > 35.0)
                .map(r -> String.format("告警: 传感器 %s 温度过高: %.2f°C",
                        r.getSensorId(), r.getTemperature()));

        alertStream.print("高温告警");
    }

    /**
     * 演示 KeyBy 分区操作
     *
     * KeyBy 是 Flink 中最重要的操作之一：
     * - 将数据流按照指定的 key 进行分区
     * - 相同 key 的数据会被发送到同一个并行实例
     * - 是进行聚合、窗口等操作的前提
     *
     * @param env Flink 执行环境
     */
    public void demonstrateKeyBy(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== KeyBy 分区操作演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== KeyBy 操作 ====================
        /*
         * 按传感器 ID 分区
         * 之后可以进行聚合操作
         */
        // 方式 1：使用 KeySelector
        var keyedStream1 = sensorStream
                .keyBy(reading -> reading.getSensorId());

        // 方式 2：使用 lambda（与方式1等价）
        var keyedStream2 = sensorStream
                .keyBy(SensorReading::getSensorId);

        /*
         * KeyBy 后的聚合操作
         * 计算每个传感器的最高温度
         */
        DataStream<SensorReading> maxTempPerSensor = sensorStream
                .keyBy(SensorReading::getSensorId)
                .max("temperature");  // 按 temperature 字段取最大值

        /*
         * 使用 maxBy 可以返回完整的最大值记录
         * 而不仅仅是更新最大字段
         */
        DataStream<SensorReading> maxRecordPerSensor = sensorStream
                .keyBy(SensorReading::getSensorId)
                .maxBy("temperature");

        maxTempPerSensor.print("每个传感器最高温度");
    }

    /**
     * 演示 Reduce 滚动聚合操作
     *
     * Reduce 对 KeyedStream 进行滚动聚合：
     * - 每来一条数据，就与之前的聚合结果进行合并
     * - 输出的是聚合后的结果
     * - 状态会被自动维护
     *
     * @param env Flink 执行环境
     */
    public void demonstrateReduce(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Reduce 聚合操作演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== Reduce 操作 ====================
        /*
         * 计算每个传感器的累计温度和
         * 用于计算平均温度等场景
         */
        DataStream<SensorReading> sumTempPerSensor = sensorStream
                .keyBy(SensorReading::getSensorId)
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading current, SensorReading newReading) {
                        // current: 之前的聚合结果
                        // newReading: 新来的数据
                        return SensorReading.builder()
                                .sensorId(current.getSensorId())
                                .timestamp(newReading.getTimestamp())
                                // 累加温度
                                .temperature(current.getTemperature() + newReading.getTemperature())
                                .build();
                    }
                });

        // 使用 Lambda 表达式
        DataStream<SensorReading> maxTempLambda = sensorStream
                .keyBy(SensorReading::getSensorId)
                .reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r1 : r2);

        sumTempPerSensor.print("累计温度");
    }

    /**
     * 演示 Union 合并流操作
     *
     * Union 可以合并多个类型相同的流
     * - 合并后的流包含所有输入流的元素
     * - 不会进行去重
     * - 元素顺序不保证
     *
     * @param env Flink 执行环境
     */
    public void demonstrateUnion(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Union 合并流演示 ==========");

        // 创建多个数据源（模拟不同来源的传感器数据）
        DataStream<SensorReading> stream1 = env.fromCollection(Arrays.asList(
                SensorReading.of("sensor_1", 25.0),
                SensorReading.of("sensor_1", 26.0)
        ));

        DataStream<SensorReading> stream2 = env.fromCollection(Arrays.asList(
                SensorReading.of("sensor_2", 30.0),
                SensorReading.of("sensor_2", 31.0)
        ));

        DataStream<SensorReading> stream3 = env.fromCollection(Arrays.asList(
                SensorReading.of("sensor_3", 35.0),
                SensorReading.of("sensor_3", 36.0)
        ));

        // ==================== Union 操作 ====================
        /*
         * 合并三个传感器数据流
         * 注意：union 可以接受多个流作为参数
         */
        DataStream<SensorReading> unionStream = stream1
                .union(stream2)
                .union(stream3);

        // 或者一次性合并
        DataStream<SensorReading> unionAllStream = stream1.union(stream2, stream3);

        unionStream.print("合并后的流");
    }

    /**
     * 演示 Connect 连接不同类型的流
     *
     * Connect 可以连接两个不同类型的流：
     * - 生成 ConnectedStreams
     * - 两个流共享状态
     * - 需要使用 CoMapFunction 或 CoFlatMapFunction 处理
     *
     * @param env Flink 执行环境
     */
    public void demonstrateConnect(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Connect 连接流演示 ==========");

        // 温度数据流
        DataStream<SensorReading> tempStream = createSensorSource(env);

        // 阈值配置流（不同类型）
        DataStream<Tuple2<String, Double>> thresholdStream = env.fromCollection(Arrays.asList(
                Tuple2.of("sensor_1", 30.0),
                Tuple2.of("sensor_2", 35.0)
        ));

        // ==================== Connect 操作 ====================
        /*
         * 连接温度流和阈值流
         * 实现动态阈值告警
         */
        DataStream<String> alertStream = tempStream
                .connect(thresholdStream)
                .flatMap(new CoFlatMapFunction<SensorReading, Tuple2<String, Double>, String>() {
                    // 存储阈值的状态（这里简化处理，实际应该用 State）
                    private final java.util.Map<String, Double> thresholds = new java.util.HashMap<>();

                    @Override
                    public void flatMap1(SensorReading reading, Collector<String> out) {
                        // 处理温度数据
                        Double threshold = thresholds.getOrDefault(reading.getSensorId(), 30.0);
                        if (reading.getTemperature() > threshold) {
                            out.collect(String.format("告警: %s 温度 %.2f 超过阈值 %.2f",
                                    reading.getSensorId(), reading.getTemperature(), threshold));
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<String, Double> threshold, Collector<String> out) {
                        // 处理阈值配置，更新状态
                        thresholds.put(threshold.f0, threshold.f1);
                        out.collect("更新阈值: " + threshold.f0 + " -> " + threshold.f1);
                    }
                });

        alertStream.print("动态阈值告警");
    }

    /**
     * 演示物理分区操作
     *
     * 物理分区决定了数据如何分配到下游算子的并行实例：
     * - shuffle: 随机分配
     * - rebalance: 轮询分配（均匀分布）
     * - rescale: 本地轮询（减少网络传输）
     * - broadcast: 广播到所有实例
     * - global: 发送到第一个实例
     * - partitionCustom: 自定义分区
     *
     * @param env Flink 执行环境
     */
    public void demonstratePartitioning(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 物理分区操作演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 各种分区策略 ====================

        // 1. Shuffle - 随机分区
        // 适用于需要打散数据的场景
        DataStream<SensorReading> shuffled = sensorStream.shuffle();

        // 2. Rebalance - 轮询分区
        // 适用于数据倾斜时重新均衡
        DataStream<SensorReading> rebalanced = sensorStream.rebalance();

        // 3. Rescale - 本地轮询
        // 只在本地 TaskManager 内轮询，减少网络开销
        DataStream<SensorReading> rescaled = sensorStream.rescale();

        // 4. Broadcast - 广播
        // 将数据发送到所有下游实例
        // 适用于小数据集与大数据集的 join
        DataStream<SensorReading> broadcasted = sensorStream.broadcast();

        // 5. Global - 发送到第一个分区
        // 适用于需要全局排序的场景（但会产生性能问题）
        DataStream<SensorReading> global = sensorStream.global();

        // 6. 自定义分区
        DataStream<SensorReading> customPartitioned = sensorStream
                .partitionCustom(
                        (key, numPartitions) -> Math.abs(key.hashCode()) % numPartitions,
                        SensorReading::getSensorId
                );

        rebalanced.print("重平衡后");
    }

    /**
     * 演示 RichFunction 的使用
     *
     * RichFunction 提供了更多功能：
     * - open(): 初始化方法，只调用一次
     * - close(): 清理方法，只调用一次
     * - getRuntimeContext(): 获取运行时上下文
     *   - 获取并行度信息
     *   - 获取状态
     *   - 获取广播变量
     *
     * @param env Flink 执行环境
     */
    public void demonstrateRichFunction(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== RichFunction 演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== RichMapFunction ====================
        DataStream<String> result = sensorStream
                .map(new RichMapFunction<SensorReading, String>() {

                    // 子任务索引（0, 1, 2, ...）
                    private int subtaskIndex;
                    // 并行度
                    private int parallelism;
                    // 计数器
                    private long count = 0;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        // 初始化方法 - 获取运行时信息
                        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                        parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
                        System.out.printf("子任务 %d/%d 初始化完成%n", subtaskIndex, parallelism);
                    }

                    @Override
                    public String map(SensorReading reading) throws Exception {
                        count++;
                        return String.format("[子任务%d] 处理第 %d 条数据: %s - %.2f°C",
                                subtaskIndex, count, reading.getSensorId(), reading.getTemperature());
                    }

                    @Override
                    public void close() throws Exception {
                        // 清理方法
                        System.out.printf("子任务 %d 处理完成，共处理 %d 条数据%n", subtaskIndex, count);
                    }
                });

        result.print("RichFunction结果");
    }

    /**
     * 创建传感器数据源
     *
     * 生成模拟的传感器数据流
     * 包含 Watermark 策略配置
     */
    private DataStream<SensorReading> createSensorSource(StreamExecutionEnvironment env) {
        Random random = new Random();

        // 生成测试数据
        List<SensorReading> sensorData = Arrays.asList(
                SensorReading.builder()
                        .sensorId("sensor_1")
                        .timestamp(System.currentTimeMillis())
                        .temperature(25.0 + random.nextDouble() * 15)
                        .humidity(50.0)
                        .location("room_1")
                        .build(),
                SensorReading.builder()
                        .sensorId("sensor_2")
                        .timestamp(System.currentTimeMillis())
                        .temperature(28.0 + random.nextDouble() * 10)
                        .humidity(55.0)
                        .location("room_2")
                        .build(),
                SensorReading.builder()
                        .sensorId("sensor_1")
                        .timestamp(System.currentTimeMillis() + 1000)
                        .temperature(30.0 + random.nextDouble() * 10)
                        .humidity(52.0)
                        .location("room_1")
                        .build(),
                SensorReading.builder()
                        .sensorId("sensor_3")
                        .timestamp(System.currentTimeMillis() + 2000)
                        .temperature(22.0 + random.nextDouble() * 8)
                        .humidity(48.0)
                        .location("room_3")
                        .build()
        );

        // 创建数据流并分配 Watermark
        return env.fromCollection(sensorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 允许 5 秒的乱序
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                // 指定事件时间字段
                                .withTimestampAssigner((reading, timestamp) -> reading.getTimestamp())
                );
    }

    /**
     * 运行所有演示
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    DataStream API 核心操作演示");
        System.out.println("=".repeat(60));

        // 注意：为了演示，每个方法都会创建新的数据流
        // 实际使用时应该在一个作业中处理

        demonstrateMap(env);
        demonstrateFlatMap(env);
        demonstrateFilter(env);
        demonstrateKeyBy(env);
        demonstrateReduce(env);
        demonstrateUnion(env);
        demonstrateRichFunction(env);

        // 执行作业
        env.execute("DataStream Basic Demo");
    }
}
