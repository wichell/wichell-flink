package com.wichell.flink.demo.window;

import com.wichell.flink.function.TemperatureAverageAggregate;
import com.wichell.flink.model.SensorReading;
import com.wichell.flink.source.SensorSourceFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static com.wichell.flink.util.FlinkUtils.formatTimestamp;

/**
 * Flink 窗口操作详细演示
 *
 * 窗口（Window）是流处理中的核心概念，用于将无界流切分为有界的数据块进行处理。
 *
 * ==================== 窗口类型 ====================
 *
 * 1. 滚动窗口（Tumbling Window）
 *    - 固定大小，窗口之间没有重叠
 *    - 每个元素只属于一个窗口
 *    - 适用于：统计每分钟/每小时的指标
 *
 * 2. 滑动窗口（Sliding Window）
 *    - 固定大小，窗口之间有重叠
 *    - 每个元素可能属于多个窗口
 *    - 适用于：计算最近 N 分钟的移动平均
 *
 * 3. 会话窗口（Session Window）
 *    - 动态大小，基于活动间隙
 *    - 不活动超过间隙时间则关闭窗口
 *    - 适用于：用户会话分析
 *
 * 4. 全局窗口（Global Window）
 *    - 所有数据放入同一个窗口
 *    - 需要自定义 Trigger 触发计算
 *
 * ==================== 时间语义 ====================
 *
 * 1. 事件时间（Event Time）- 推荐
 *    - 数据自带的时间戳
 *    - 结果可重现，不受处理延迟影响
 *
 * 2. 处理时间（Processing Time）
 *    - 算子处理数据时的系统时间
 *    - 延迟最低，但结果不可重现
 *
 * @author wichell
 */
@Slf4j
@Component
public class WindowDemo {

    /**
     * 演示滚动窗口（Tumbling Window）
     *
     * 滚动窗口的特点：
     * - 窗口大小固定
     * - 窗口之间没有重叠
     * - 每个元素只属于一个窗口
     *
     * 使用场景：
     * - 每分钟的 PV/UV 统计
     * - 每小时的销售额统计
     * - 每天的日报生成
     */
    public void demonstrateTumblingWindow(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("滚动窗口演示");

        // 创建传感器数据源
        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 基于时间的滚动窗口 ====================
        /*
         * 每 10 秒统计一次每个传感器的平均温度
         *
         * 窗口划分示例（假设从 00:00 开始）：
         * [00:00, 00:10) -> 第一个窗口
         * [00:10, 00:20) -> 第二个窗口
         * [00:20, 00:30) -> 第三个窗口
         */
        SingleOutputStreamOperator<String> tumblingResult = sensorStream
                // 按传感器 ID 分组
                .keyBy(SensorReading::getSensorId)
                // 定义 10 秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 使用 ProcessWindowFunction 处理窗口数据
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String sensorId,
                                        Context context,
                                        Iterable<SensorReading> readings,
                                        Collector<String> out) {
                        // 计算平均温度
                        double sum = 0;
                        int count = 0;
                        for (SensorReading reading : readings) {
                            sum += reading.getTemperature();
                            count++;
                        }
                        double avg = count > 0 ? sum / count : 0;

                        // 获取窗口信息
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        String startTime = formatTimestamp(start);
                        String endTime = formatTimestamp(end);

                        out.collect(String.format(
                                "[滚动窗口] 传感器: %s, 窗口: [%s - %s), 数据量: %d, 平均温度: %.2f°C",
                                sensorId, startTime, endTime, count, avg
                        ));
                    }
                });

        tumblingResult.print();

        // ==================== 基于处理时间的滚动窗口 ====================
        /*
         * 使用处理时间（Processing Time）
         * 适用于对实时性要求高，对精确性要求相对较低的场景
         */
        DataStream<SensorReading> processTimeStream = createProcessingTimeSource(env);

        SingleOutputStreamOperator<String> processingTimeResult = processTimeStream
                .keyBy(SensorReading::getSensorId)
                // 使用处理时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading r1, SensorReading r2) {
                        // 取最高温度
                        return r1.getTemperature() > r2.getTemperature() ? r1 : r2;
                    }
                })
                .map(r -> String.format("[处理时间窗口] %s 最高温度: %.2f°C",
                        r.getSensorId(), r.getTemperature()));

        // ==================== 基于计数的滚动窗口 ====================
        /*
         * 每收集 5 条数据触发一次计算
         * 适用于按数据量而非时间触发的场景
         */
        SingleOutputStreamOperator<String> countWindowResult = processTimeStream
                .keyBy(SensorReading::getSensorId)
                // 每 5 条数据一个窗口
                .countWindow(5)
                .reduce((r1, r2) -> SensorReading.builder()
                        .sensorId(r1.getSensorId())
                        .temperature(r1.getTemperature() + r2.getTemperature())
                        .build())
                .map(r -> String.format("[计数窗口] %s 累计温度: %.2f°C",
                        r.getSensorId(), r.getTemperature()));

        countWindowResult.print();
    }

    /**
     * 演示滑动窗口（Sliding Window）
     *
     * 滑动窗口的特点：
     * - 窗口大小固定
     * - 窗口之间有重叠（滑动步长 < 窗口大小）
     * - 每个元素可能属于多个窗口
     *
     * 使用场景：
     * - 计算最近 5 分钟的移动平均
     * - 滚动计算最近 1 小时的 Top N
     */
    public void demonstrateSlidingWindow(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("滑动窗口演示");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 时间滑动窗口 ====================
        /*
         * 窗口大小：30 秒
         * 滑动步长：10 秒
         *
         * 窗口划分示例（假设从 00:00 开始）：
         * [00:00, 00:30) -> 第一个窗口
         * [00:10, 00:40) -> 第二个窗口（与第一个重叠 20 秒）
         * [00:20, 00:50) -> 第三个窗口（与第二个重叠 20 秒）
         *
         * 00:15 的数据会被分配到窗口 1 和窗口 2
         */
        SingleOutputStreamOperator<String> slidingResult = sensorStream
                .keyBy(SensorReading::getSensorId)
                // 窗口大小 30 秒，滑动步长 10 秒
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(
                        // 使用增量聚合（AggregateFunction）提高效率
                        new TemperatureAverageAggregate(),
                        // 使用 ProcessWindowFunction 获取窗口信息
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String sensorId,
                                                Context context,
                                                Iterable<Double> avgTemps,
                                                Collector<String> out) {
                                Double avgTemp = avgTemps.iterator().next();
                                out.collect(String.format(
                                        "[滑动窗口] 传感器: %s, 窗口: [%s - %s), 平均温度: %.2f°C",
                                        sensorId,
                                        formatTimestamp(context.window().getStart()),
                                        formatTimestamp(context.window().getEnd()),
                                        avgTemp
                                ));
                            }
                        }
                );

        slidingResult.print();

        // ==================== 计数滑动窗口 ====================
        /*
         * 每收集 3 条新数据，计算最近 10 条数据的统计
         */
        SingleOutputStreamOperator<String> countSlidingResult = createProcessingTimeSource(env)
                .keyBy(SensorReading::getSensorId)
                // 窗口大小 10 条，滑动步长 3 条
                .countWindow(10, 3)
                .process(new ProcessWindowFunction<SensorReading, String, String, org.apache.flink.streaming.api.windowing.windows.GlobalWindow>() {
                    @Override
                    public void process(String sensorId,
                                        Context context,
                                        Iterable<SensorReading> readings,
                                        Collector<String> out) {
                        double max = Double.MIN_VALUE;
                        double min = Double.MAX_VALUE;
                        int count = 0;
                        for (SensorReading r : readings) {
                            max = Math.max(max, r.getTemperature());
                            min = Math.min(min, r.getTemperature());
                            count++;
                        }
                        out.collect(String.format(
                                "[计数滑动窗口] %s: 最近%d条, 最高%.2f°C, 最低%.2f°C",
                                sensorId, count, max, min
                        ));
                    }
                });
        countSlidingResult.print();
    }

    /**
     * 演示会话窗口（Session Window）
     *
     * 会话窗口的特点：
     * - 窗口大小动态变化
     * - 基于活动间隙（gap）来划分窗口
     * - 如果两个事件之间的间隔超过 gap，则属于不同窗口
     *
     * 使用场景：
     * - 用户会话分析（用户行为序列）
     * - 交易会话统计
     */
    public void demonstrateSessionWindow(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("会话窗口演示");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 固定间隙的会话窗口 ====================
        /*
         * 间隙：10 秒
         *
         * 如果传感器数据间隔超过 10 秒，则认为是新的会话
         * 适用于分析传感器的活动周期
         */
        SingleOutputStreamOperator<String> sessionResult = sensorStream
                .keyBy(SensorReading::getSensorId)
                // 会话间隙 10 秒
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String sensorId,
                                        Context context,
                                        Iterable<SensorReading> readings,
                                        Collector<String> out) {
                        int count = 0;
                        double sum = 0;
                        long firstTime = Long.MAX_VALUE;
                        long lastTime = Long.MIN_VALUE;

                        for (SensorReading r : readings) {
                            count++;
                            sum += r.getTemperature();
                            firstTime = Math.min(firstTime, r.getTimestamp());
                            lastTime = Math.max(lastTime, r.getTimestamp());
                        }

                        long duration = lastTime - firstTime;

                        out.collect(String.format(
                                "[会话窗口] 传感器: %s, 窗口: [%s - %s), 持续: %d秒, 数据量: %d, 平均温度: %.2f°C",
                                sensorId,
                                formatTimestamp(context.window().getStart()),
                                formatTimestamp(context.window().getEnd()),
                                duration / 1000,
                                count,
                                count > 0 ? sum / count : 0
                        ));
                    }
                });

        sessionResult.print();

        // ==================== 动态间隙的会话窗口 ====================
        /*
         * 根据传感器 ID 动态设置不同的间隙
         * 例如：重要传感器使用较短间隙，次要传感器使用较长间隙
         */
        SingleOutputStreamOperator<String> dynamicSessionResult = sensorStream
                .keyBy(SensorReading::getSensorId)
                .window(EventTimeSessionWindows.withDynamicGap(
                        (reading) -> {
                            // 根据传感器 ID 动态设置间隙
                            if (reading.getSensorId().contains("_1")) {
                                return 5000L;  // sensor_1 使用 5 秒间隙
                            } else {
                                return 15000L; // 其他传感器使用 15 秒间隙
                            }
                        }
                ))
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String sensorId,
                                        Context context,
                                        Iterable<SensorReading> readings,
                                        Collector<String> out) {
                        int count = 0;
                        for (SensorReading r : readings) {
                            count++;
                        }
                        out.collect(String.format("[动态会话窗口] %s: %d 条数据", sensorId, count));
                    }
                });
        dynamicSessionResult.print();
    }

    /**
     * 演示窗口函数（Window Functions）
     *
     * Flink 提供了多种窗口函数：
     *
     * 1. ReduceFunction - 增量聚合
     *    - 每来一条数据就进行聚合
     *    - 效率高，但只能聚合为相同类型
     *
     * 2. AggregateFunction - 增量聚合（更灵活）
     *    - 支持不同的输入、累加器、输出类型
     *    - 效率高，可实现复杂聚合
     *
     * 3. ProcessWindowFunction - 全量处理
     *    - 收集窗口所有数据后处理
     *    - 可以访问窗口元数据
     *    - 内存占用较高
     *
     * 4. 增量聚合 + ProcessWindowFunction - 最佳实践
     *    - 结合两者优点
     *    - 增量聚合提高效率
     *    - ProcessWindowFunction 获取窗口信息
     */
    public void demonstrateWindowFunctions(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("窗口函数演示");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 1. ReduceFunction ====================
        /*
         * 增量聚合，计算最大温度
         * 每来一条数据就更新最大值
         */
        DataStream<SensorReading> maxTempStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading r1, SensorReading r2) {
                        return r1.getTemperature() > r2.getTemperature() ? r1 : r2;
                    }
                });
        maxTempStream.print("最大温度");

        // ==================== 2. AggregateFunction ====================
        /*
         * 更灵活的增量聚合
         * 计算平均温度
         *
         * AggregateFunction<IN, ACC, OUT>
         * - IN: 输入类型
         * - ACC: 累加器类型
         * - OUT: 输出类型
         */
        DataStream<Tuple2<String, Double>> avgTempStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<SensorReading, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {

                    // 创建累加器
                    @Override
                    public Tuple3<String, Double, Integer> createAccumulator() {
                        return Tuple3.of("", 0.0, 0);
                    }

                    // 累加每条数据
                    @Override
                    public Tuple3<String, Double, Integer> add(SensorReading reading,
                                                                Tuple3<String, Double, Integer> acc) {
                        return Tuple3.of(
                                reading.getSensorId(),
                                acc.f1 + reading.getTemperature(),
                                acc.f2 + 1
                        );
                    }

                    // 获取最终结果
                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> acc) {
                        double avg = acc.f2 > 0 ? acc.f1 / acc.f2 : 0;
                        return Tuple2.of(acc.f0, avg);
                    }

                    // 合并累加器（用于会话窗口合并）
                    @Override
                    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc1,
                                                                  Tuple3<String, Double, Integer> acc2) {
                        return Tuple3.of(
                                acc1.f0,
                                acc1.f1 + acc2.f1,
                                acc1.f2 + acc2.f2
                        );
                    }
                });
        avgTempStream.print("平均温度");

        // ==================== 3. 增量聚合 + ProcessWindowFunction ====================
        /*
         * 最佳实践：结合增量聚合的效率和 ProcessWindowFunction 的窗口信息
         */
        SingleOutputStreamOperator<String> combinedResult = sensorStream
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        // 增量计算平均值
                        new TemperatureAverageAggregate(),
                        // 添加窗口信息
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String sensorId,
                                                Context context,
                                                Iterable<Double> avgTemps,
                                                Collector<String> out) {
                                Double avgTemp = avgTemps.iterator().next();
                                out.collect(String.format(
                                        "传感器: %s, 窗口: [%s, %s), 平均温度: %.2f°C",
                                        sensorId,
                                        formatTimestamp(context.window().getStart()),
                                        formatTimestamp(context.window().getEnd()),
                                        avgTemp
                                ));
                            }
                        }
                );

        combinedResult.print("窗口聚合结果");
    }

    /**
     * 演示 Watermark 和延迟数据处理
     *
     * Watermark 是 Flink 处理事件时间的核心机制：
     * - 表示"不会再有时间戳小于 Watermark 的数据到达"
     * - 用于触发窗口计算
     * - 处理乱序数据
     *
     * 延迟数据处理策略：
     * 1. 允许一定的乱序（forBoundedOutOfOrderness）
     * 2. 使用 allowedLateness 允许额外延迟
     * 3. 使用侧输出流收集超时数据
     */
    public void demonstrateWatermarkAndLateness(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("Watermark 和延迟数据处理演示");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // 定义侧输出标签，用于收集延迟数据
        final org.apache.flink.util.OutputTag<SensorReading> lateDataTag =
                new org.apache.flink.util.OutputTag<SensorReading>("late-data") {};

        // ==================== 处理延迟数据 ====================
        SingleOutputStreamOperator<String> resultStream = sensorStream
                .keyBy(SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 允许 5 秒的延迟
                .allowedLateness(Time.seconds(5))
                // 将超过延迟时间的数据输出到侧输出流
                .sideOutputLateData(lateDataTag)
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String sensorId,
                                        Context context,
                                        Iterable<SensorReading> readings,
                                        Collector<String> out) {
                        int count = 0;
                        double sum = 0;
                        for (SensorReading r : readings) {
                            count++;
                            sum += r.getTemperature();
                        }
                        out.collect(String.format(
                                "窗口 [%s, %s): %s 平均温度 %.2f°C (共%d条)",
                                formatTimestamp(context.window().getStart()),
                                formatTimestamp(context.window().getEnd()),
                                sensorId,
                                count > 0 ? sum / count : 0,
                                count
                        ));
                    }
                });

        // 获取延迟数据
        DataStream<SensorReading> lateDataStream = resultStream.getSideOutput(lateDataTag);

        resultStream.print("正常数据");
        lateDataStream.map(r -> "延迟数据: " + r.getSensorId() + " - " + r.getTemperature())
                .print("延迟数据");
    }

    // ==================== 辅助方法 ====================


    /**
     * 创建带有事件时间的传感器数据源
     */
    private DataStream<SensorReading> createSensorSource(StreamExecutionEnvironment env) {
        return env.addSource(SensorSourceFunction.builder().build())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 允许 5 秒的乱序
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                // 指定事件时间字段
                                .withTimestampAssigner((reading, ts) -> reading.getTimestamp())
                                // 5 分钟没有数据则标记为空闲
                                .withIdleness(Duration.ofMinutes(5))
                );
    }

    /**
     * 创建处理时间数据源（无 Watermark）
     */
    private DataStream<SensorReading> createProcessingTimeSource(StreamExecutionEnvironment env) {
        return env.addSource(SensorSourceFunction.builder().build());
    }

    /**
     * 运行指定的窗口演示
     *
     * @param env Flink 执行环境
     * @param demoName 要运行的演示名称，可选值：
     *                 - "tumbling": 滚动窗口
     *                 - "sliding": 滑动窗口
     *                 - "session": 会话窗口
     *                 - "functions": 窗口函数
     *                 - "watermark": Watermark 和延迟数据
     */
    public void runDemo(StreamExecutionEnvironment env, String demoName) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("Flink 窗口操作演示 - " + demoName);

        switch (demoName.toLowerCase()) {
                case "tumbling":
                demonstrateTumblingWindow(env);
                break;
            case "sliding":
                demonstrateSlidingWindow(env);
                break;
            case "session":
                demonstrateSessionWindow(env);
                break;
            case "functions":
                demonstrateWindowFunctions(env);
                break;
            case "watermark":
                demonstrateWatermarkAndLateness(env);
                break;
            default:
                log.error("未知的演示名称: {}，可选值: tumbling, sliding, session, functions, watermark", demoName);
                return;
        }

        env.execute("Window Demo - " + demoName);
    }

    /**
     * 运行所有窗口演示
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("Flink 窗口操作演示");

        // 默认运行 Watermark 演示
        // 可根据需要修改为其他演示
        runDemo(env, "watermark");

        env.execute("Window Demo");
    }
}
