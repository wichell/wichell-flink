package com.wichell.flink.demo.tableapi;

import com.wichell.flink.model.Order;
import com.wichell.flink.model.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink Table API 和 SQL 详细演示
 *
 * Table API 和 SQL 是 Flink 的声明式 API，提供了关系型数据处理能力。
 *
 * ==================== 核心概念 ====================
 *
 * 1. TableEnvironment
 *    - Table API 的入口
 *    - 用于注册表、执行 SQL
 *
 * 2. Table
 *    - 表示一个关系表
 *    - 可以通过 Table API 或 SQL 进行操作
 *
 * 3. Catalog
 *    - 表的元数据管理
 *    - 支持多种 Catalog（如 Hive Catalog）
 *
 * ==================== 与 DataStream 的转换 ====================
 *
 * 1. DataStream -> Table
 *    - fromDataStream()
 *    - createTemporaryView()
 *
 * 2. Table -> DataStream
 *    - toDataStream()
 *    - toChangelogStream()
 *
 * ==================== 时间属性 ====================
 *
 * 1. 处理时间（Processing Time）
 *    - 使用 proctime() 函数定义
 *
 * 2. 事件时间（Event Time）
 *    - 从 DataStream 的 Watermark 继承
 *    - 或在 DDL 中使用 WATERMARK 定义
 *
 * @author wichell
 */
@Component
public class TableApiDemo {

    /**
     * 演示 Table API 基本操作
     *
     * Table API 提供了类似于 SQL 的操作：
     * - select: 选择列
     * - where/filter: 过滤
     * - groupBy: 分组
     * - orderBy: 排序
     * - join: 连接
     * - union: 合并
     */
    public void demonstrateTableApiBasics(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Table API 基本操作演示 ==========");

        // ==================== 1. 创建 TableEnvironment ====================
        /*
         * StreamTableEnvironment 用于流处理
         * 它是 DataStream API 和 Table API 之间的桥梁
         */
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 从 DataStream 创建 Table ====================
        DataStream<SensorReading> sensorStream = createSensorSource(env);

        /*
         * 方式 1: 使用 fromDataStream
         * 自动推断 schema，字段名来自 POJO 属性
         */
        Table sensorTable = tableEnv.fromDataStream(sensorStream);

        /*
         * 方式 2: 使用 fromDataStream 并自定义列
         * 可以重命名列、添加时间属性等
         */
        Table sensorTableWithTime = tableEnv.fromDataStream(
                sensorStream,
                Schema.newBuilder()
                        .column("sensorId", DataTypes.STRING())
                        .column("temperature", DataTypes.DOUBLE())
                        .column("humidity", DataTypes.DOUBLE())
                        .column("timestamp", DataTypes.BIGINT())
                        // 添加处理时间列
                        .columnByExpression("proc_time", "PROCTIME()")
                        // 添加事件时间列
                        .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(`timestamp`, 3)")
                        .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                        .build()
        );

        /*
         * 方式 3: 注册为临时视图，可以在 SQL 中使用
         */
        tableEnv.createTemporaryView("sensors", sensorTable);

        // ==================== 3. Table API 操作 ====================

        // 3.1 Select - 选择列
        Table selectedTable = sensorTable
                .select($("sensorId"), $("temperature"));

        // 3.2 Filter/Where - 过滤
        Table filteredTable = sensorTable
                .where($("temperature").isGreater(30.0));

        // 3.3 添加计算列
        Table computedTable = sensorTable
                .addColumns(
                        $("temperature").times(1.8).plus(32).as("fahrenheit")
                );

        // 3.4 GroupBy 聚合
        Table aggregatedTable = sensorTable
                .groupBy($("sensorId"))
                .select(
                        $("sensorId"),
                        $("temperature").avg().as("avg_temp"),
                        $("temperature").max().as("max_temp"),
                        $("temperature").min().as("min_temp"),
                        $("temperature").count().as("count")
                );

        // 3.5 排序（仅在批处理模式有效）
        // Table orderedTable = sensorTable.orderBy($("temperature").desc());

        // ==================== 4. 输出结果 ====================

        // 转换回 DataStream 并打印
        tableEnv.toDataStream(filteredTable)
                .print("过滤后");

        tableEnv.toChangelogStream(aggregatedTable)
                .print("聚合结果");
    }

    /**
     * 演示 Flink SQL
     *
     * Flink SQL 完全兼容 ANSI SQL 标准，支持：
     * - DDL: CREATE, DROP, ALTER
     * - DML: SELECT, INSERT
     * - 窗口聚合
     * - 连接操作
     * - 子查询
     */
    public void demonstrateSql(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Flink SQL 演示 ==========");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 1. 使用 DDL 创建表 ====================
        /*
         * 使用 SQL DDL 创建表
         * 可以指定：
         * - 列定义
         * - Watermark
         * - 连接器配置
         */

        // 创建 Datagen 源表（用于生成测试数据）
        tableEnv.executeSql("""
            CREATE TABLE sensor_source (
                sensor_id STRING,
                temperature DOUBLE,
                humidity DOUBLE,
                ts TIMESTAMP(3),
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '5',
                'fields.sensor_id.kind' = 'random',
                'fields.sensor_id.length' = '10',
                'fields.temperature.kind' = 'random',
                'fields.temperature.min' = '20.0',
                'fields.temperature.max' = '40.0',
                'fields.humidity.kind' = 'random',
                'fields.humidity.min' = '30.0',
                'fields.humidity.max' = '80.0'
            )
        """);

        // 创建 Print Sink 表
        tableEnv.executeSql("""
            CREATE TABLE sensor_sink (
                sensor_id STRING,
                avg_temp DOUBLE,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3)
            ) WITH (
                'connector' = 'print'
            )
        """);

        // ==================== 2. 执行 SQL 查询 ====================

        // 2.1 简单查询
        Table simpleResult = tableEnv.sqlQuery("""
            SELECT sensor_id, temperature, humidity
            FROM sensor_source
            WHERE temperature > 30.0
        """);

        // 2.2 窗口聚合查询
        /*
         * 滚动窗口聚合
         * 使用 TUMBLE 函数定义窗口
         */
        TableResult windowResult = tableEnv.executeSql("""
            INSERT INTO sensor_sink
            SELECT
                sensor_id,
                AVG(temperature) as avg_temp,
                TUMBLE_START(ts, INTERVAL '10' SECOND) as window_start,
                TUMBLE_END(ts, INTERVAL '10' SECOND) as window_end
            FROM sensor_source
            GROUP BY
                sensor_id,
                TUMBLE(ts, INTERVAL '10' SECOND)
        """);

        // 2.3 其他窗口类型示例
        /*
         * 滑动窗口:
         * HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)
         *
         * 会话窗口:
         * SESSION(ts, INTERVAL '5' MINUTE)
         *
         * 累计窗口:
         * CUMULATE(ts, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)
         */

        System.out.println("SQL 查询已提交执行");
    }

    /**
     * 演示 DataStream 和 Table 的互相转换
     *
     * 这是 Flink 混合使用 DataStream API 和 Table API 的关键
     */
    public void demonstrateStreamTableConversion(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== DataStream 与 Table 转换演示 ==========");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 1. DataStream -> Table ====================

        // 方式 1: 自动推断 schema
        Table table1 = tableEnv.fromDataStream(sensorStream);

        // 方式 2: 显式指定 schema
        Table table2 = tableEnv.fromDataStream(
                sensorStream,
                $("sensorId").as("sensor_id"),
                $("temperature"),
                $("timestamp").as("ts")
        );

        // 方式 3: 注册为视图
        tableEnv.createTemporaryView("sensor_view", sensorStream,
                $("sensorId"), $("temperature"), $("timestamp"));

        // ==================== 2. Table -> DataStream ====================

        // 使用 Table API 处理
        Table processedTable = table1
                .select($("sensorId"), $("temperature"))
                .where($("temperature").isGreater(25.0));

        // 方式 1: toDataStream - 用于只追加的表
        /*
         * 适用于：
         * - SELECT without GROUP BY
         * - 不更新已输出的行
         */
        DataStream<Row> appendStream = tableEnv.toDataStream(processedTable);

        // 方式 2: toChangelogStream - 用于有更新的表
        /*
         * 适用于：
         * - SELECT with GROUP BY
         * - 可能更新或删除已输出的行
         *
         * 输出的 Row 带有 RowKind:
         * - INSERT: 新增行
         * - UPDATE_BEFORE: 更新前的旧值
         * - UPDATE_AFTER: 更新后的新值
         * - DELETE: 删除行
         */
        Table aggregatedTable = table1
                .groupBy($("sensorId"))
                .select($("sensorId"), $("temperature").avg().as("avg_temp"));

        DataStream<Row> changelogStream = tableEnv.toChangelogStream(aggregatedTable);

        // 打印结果
        appendStream.print("追加流");
        changelogStream.print("更新流");
    }

    /**
     * 演示窗口 TVF (Table-Valued Functions)
     *
     * Flink 1.13+ 推荐使用 TVF 定义窗口，更灵活且功能更强
     */
    public void demonstrateWindowTvf(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 窗口 TVF 演示 ==========");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建源表
        tableEnv.executeSql("""
            CREATE TABLE orders (
                order_id STRING,
                user_id STRING,
                amount DECIMAL(10, 2),
                order_time TIMESTAMP(3),
                WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '10',
                'fields.order_id.length' = '8',
                'fields.user_id.length' = '5',
                'fields.amount.min' = '10.0',
                'fields.amount.max' = '1000.0'
            )
        """);

        // ==================== 1. TUMBLE 滚动窗口 TVF ====================
        Table tumbleResult = tableEnv.sqlQuery("""
            SELECT
                window_start,
                window_end,
                user_id,
                SUM(amount) as total_amount,
                COUNT(*) as order_count
            FROM TABLE(
                TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' MINUTE)
            )
            GROUP BY window_start, window_end, user_id
        """);

        // ==================== 2. HOP 滑动窗口 TVF ====================
        Table hopResult = tableEnv.sqlQuery("""
            SELECT
                window_start,
                window_end,
                user_id,
                AVG(amount) as avg_amount
            FROM TABLE(
                HOP(TABLE orders, DESCRIPTOR(order_time),
                    INTERVAL '30' SECOND,
                    INTERVAL '1' MINUTE)
            )
            GROUP BY window_start, window_end, user_id
        """);

        // ==================== 3. CUMULATE 累计窗口 TVF ====================
        Table cumulateResult = tableEnv.sqlQuery("""
            SELECT
                window_start,
                window_end,
                SUM(amount) as cumulative_amount
            FROM TABLE(
                CUMULATE(TABLE orders, DESCRIPTOR(order_time),
                    INTERVAL '10' SECOND,
                    INTERVAL '1' MINUTE)
            )
            GROUP BY window_start, window_end
        """);

        // 输出结果
        tableEnv.toChangelogStream(tumbleResult).print("滚动窗口");
    }

    /**
     * 演示用户自定义函数 (UDF)
     *
     * Flink 支持多种类型的 UDF：
     * - ScalarFunction: 标量函数，一对一转换
     * - TableFunction: 表函数，一对多转换
     * - AggregateFunction: 聚合函数
     * - TableAggregateFunction: 表聚合函数
     */
    public void demonstrateUdf(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 用户自定义函数演示 ==========");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 1. 注册 ScalarFunction ====================
        /*
         * 标量函数：一个输入，一个输出
         * 例如：温度转换（摄氏度 -> 华氏度）
         */
        tableEnv.createTemporarySystemFunction("toFahrenheit", ToFahrenheit.class);

        // ==================== 2. 注册 TableFunction ====================
        /*
         * 表函数：一个输入，多个输出（多行）
         * 例如：字符串分割
         */
        tableEnv.createTemporarySystemFunction("splitString", SplitFunction.class);

        // ==================== 3. 注册 AggregateFunction ====================
        /*
         * 聚合函数：多个输入，一个输出
         * 例如：加权平均
         */
        tableEnv.createTemporarySystemFunction("weightedAvg", WeightedAvgFunction.class);

        // 创建测试数据
        DataStream<SensorReading> sensorStream = createSensorSource(env);
        tableEnv.createTemporaryView("sensors", sensorStream);

        // ==================== 4. 使用 UDF ====================

        // 使用标量函数
        Table scalarResult = tableEnv.sqlQuery("""
            SELECT
                sensorId,
                temperature,
                toFahrenheit(temperature) as fahrenheit
            FROM sensors
        """);

        // 使用表函数（LATERAL TABLE）
        Table tableResult = tableEnv.sqlQuery("""
            SELECT
                sensorId,
                word
            FROM sensors,
            LATERAL TABLE(splitString(sensorId, '_')) AS T(word)
        """);

        tableEnv.toDataStream(scalarResult).print("标量函数结果");
    }

    // ==================== 自定义函数实现 ====================

    /**
     * 标量函数：摄氏度转华氏度
     */
    public static class ToFahrenheit extends ScalarFunction {
        public Double eval(Double celsius) {
            if (celsius == null) {
                return null;
            }
            return celsius * 1.8 + 32;
        }
    }

    /**
     * 表函数：字符串分割
     */
    public static class SplitFunction extends TableFunction<String> {
        public void eval(String str, String delimiter) {
            if (str != null && delimiter != null) {
                for (String s : str.split(delimiter)) {
                    collect(s);
                }
            }
        }
    }

    /**
     * 聚合函数：加权平均
     *
     * AggregateFunction<T, ACC>
     * - T: 输出类型
     * - ACC: 累加器类型
     */
    public static class WeightedAvgFunction extends AggregateFunction<Double, WeightedAvgAccumulator> {

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        public void accumulate(WeightedAvgAccumulator acc, Double value, Integer weight) {
            if (value != null && weight != null) {
                acc.sum += value * weight;
                acc.count += weight;
            }
        }

        @Override
        public Double getValue(WeightedAvgAccumulator acc) {
            return acc.count == 0 ? null : acc.sum / acc.count;
        }
    }

    /**
     * 加权平均累加器
     */
    public static class WeightedAvgAccumulator {
        public double sum = 0;
        public int count = 0;
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
            String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};

            while (running) {
                for (String sensorId : sensorIds) {
                    ctx.collect(SensorReading.builder()
                            .sensorId(sensorId)
                            .timestamp(System.currentTimeMillis())
                            .temperature(20 + random.nextDouble() * 20)
                            .humidity(40 + random.nextDouble() * 40)
                            .location("room_" + random.nextInt(5))
                            .build());
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 运行所有 Table API 演示
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink Table API 和 SQL 演示");
        System.out.println("=".repeat(60));

        // 选择一个演示运行
        demonstrateTableApiBasics(env);
        // demonstrateSql(env);
        // demonstrateStreamTableConversion(env);
        // demonstrateWindowTvf(env);
        // demonstrateUdf(env);

        env.execute("Table API Demo");
    }

    /**
     * 异步运行所有 Table API 演示，返回 JobClient 用于作业控制
     */
    public org.apache.flink.core.execution.JobClient runAllDemosAsync(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink Table API 和 SQL 演示");
        System.out.println("=".repeat(60));

        demonstrateTableApiBasics(env);

        return env.executeAsync("Table API Demo");
    }
}
