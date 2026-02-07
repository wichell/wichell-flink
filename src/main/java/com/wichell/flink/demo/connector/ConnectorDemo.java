package com.wichell.flink.demo.connector;

import com.wichell.flink.model.Order;
import com.wichell.flink.model.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;

/**
 * Flink 连接器 (Connectors) 详细演示
 *
 * Flink 提供了丰富的连接器，用于与外部系统交互：
 * - Kafka：消息队列
 * - JDBC：关系型数据库
 * - File System：文件系统
 * - Elasticsearch：搜索引擎
 * - Redis：缓存数据库
 * - ...
 *
 * ==================== 连接器类型 ====================
 *
 * 1. Source Connector（数据源）
 *    - 从外部系统读取数据
 *    - 支持有界和无界数据源
 *
 * 2. Sink Connector（数据汇）
 *    - 将处理结果写入外部系统
 *    - 支持多种写入语义
 *
 * ==================== 写入语义 ====================
 *
 * 1. At-least-once（至少一次）
 *    - 数据至少被写入一次
 *    - 可能有重复
 *
 * 2. Exactly-once（精确一次）
 *    - 数据恰好被写入一次
 *    - 需要连接器和外部系统支持事务
 *
 * @author wichell
 */
@Component
public class ConnectorDemo {

    /**
     * 演示 Kafka Source 连接器
     *
     * Kafka Source 用于从 Kafka 消费数据：
     * - 支持精确一次语义
     * - 自动管理消费位点
     * - 支持动态分区发现
     */
    public void demonstrateKafkaSource(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Kafka Source 连接器演示 ==========");

        // ==================== Kafka Source 配置 ====================
        /*
         * 使用 KafkaSource Builder 模式配置
         *
         * 主要配置项：
         * - bootstrap.servers: Kafka 集群地址
         * - topics: 订阅的主题
         * - consumer group: 消费者组
         * - starting offset: 起始消费位点
         * - deserializer: 反序列化器
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // Kafka 集群地址
                .setBootstrapServers("localhost:9092")
                // 订阅的主题（可以是多个）
                .setTopics("sensor-data")
                // 消费者组 ID
                .setGroupId("flink-consumer-group")
                // 起始消费位点
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 值的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 其他 Kafka 配置
                .setProperty("enable.auto.commit", "false")
                .setProperty("auto.offset.reset", "earliest")
                .build();

        /*
         * 起始消费位点选项：
         * - OffsetsInitializer.earliest(): 从最早的数据开始
         * - OffsetsInitializer.latest(): 从最新的数据开始
         * - OffsetsInitializer.committedOffsets(): 从已提交的位点开始
         * - OffsetsInitializer.timestamp(timestamp): 从指定时间戳开始
         * - OffsetsInitializer.offsets(Map): 从指定位点开始
         */

        // 创建数据流
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 处理数据
        kafkaStream
                .map(json -> "接收到 Kafka 消息: " + json)
                .print("Kafka");

        System.out.println("Kafka Source 配置完成");
        System.out.println("  - Bootstrap Servers: localhost:9092");
        System.out.println("  - Topic: sensor-data");
        System.out.println("  - Consumer Group: flink-consumer-group");
    }

    /**
     * 演示 Kafka Sink 连接器
     *
     * Kafka Sink 用于将数据写入 Kafka：
     * - 支持精确一次语义（需要开启事务）
     * - 支持自定义分区
     * - 支持幂等性生产
     */
    public void demonstrateKafkaSink(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Kafka Sink 连接器演示 ==========");

        // 创建测试数据流
        DataStream<String> dataStream = createStringSource(env);

        // ==================== Kafka Sink 配置 ====================
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // Kafka 集群地址
                .setBootstrapServers("localhost:9092")
                // 记录序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("flink-output")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                // 可选：自定义分区
                                // .setPartitioner(new FlinkKafkaPartitioner<String>() {...})
                                .build()
                )
                /*
                 * 投递语义：
                 * - AT_LEAST_ONCE: 至少一次，可能重复
                 * - EXACTLY_ONCE: 精确一次，需要 Kafka 事务支持
                 * - NONE: 尽力而为，可能丢失
                 */
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                /*
                 * 精确一次语义需要额外配置：
                 * .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                 * .setTransactionalIdPrefix("flink-tx-")
                 *
                 * 并且需要配置 Kafka：
                 * - transaction.max.timeout.ms >= checkpoint interval
                 */
                .build();

        // 写入 Kafka
        dataStream.sinkTo(kafkaSink);

        System.out.println("Kafka Sink 配置完成");
        System.out.println("  - Topic: flink-output");
        System.out.println("  - Delivery Guarantee: AT_LEAST_ONCE");
    }

    /**
     * 演示 JDBC Sink 连接器
     *
     * JDBC Sink 用于将数据写入关系型数据库：
     * - 支持批量写入
     * - 支持精确一次语义（使用 XA 事务）
     * - 支持多种数据库
     */
    public void demonstrateJdbcSink(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== JDBC Sink 连接器演示 ==========");

        // 创建订单数据流
        DataStream<Order> orderStream = createOrderSource(env);

        // ==================== JDBC Sink 配置 ====================
        SinkFunction<Order> jdbcSink = JdbcSink.sink(
                // SQL 语句（使用占位符）
                "INSERT INTO orders (order_id, user_id, amount, status, create_time) " +
                        "VALUES (?, ?, ?, ?, ?)",
                // 设置参数
                (ps, order) -> {
                    ps.setString(1, order.getOrderId());
                    ps.setString(2, order.getUserId());
                    ps.setBigDecimal(3, order.getAmount());
                    ps.setString(4, order.getStatus());
                    ps.setLong(5, order.getCreateTime());
                },
                // 执行选项
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)  // 批量大小
                        .withBatchIntervalMs(200)  // 批量间隔
                        .withMaxRetries(5)  // 最大重试次数
                        .build(),
                // 连接选项
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink_demo")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        );

        // 写入数据库
        orderStream.addSink(jdbcSink);

        System.out.println("JDBC Sink 配置完成");
        System.out.println("  - URL: jdbc:mysql://localhost:3306/flink_demo");
        System.out.println("  - Table: orders");
        System.out.println("  - Batch Size: 1000");

        /*
         * 对应的建表语句：
         *
         * CREATE TABLE orders (
         *     order_id VARCHAR(50) PRIMARY KEY,
         *     user_id VARCHAR(50) NOT NULL,
         *     amount DECIMAL(10, 2),
         *     status VARCHAR(20),
         *     create_time BIGINT
         * );
         */
    }

    /**
     * 演示 File Source 连接器
     *
     * File Source 用于读取文件系统中的数据：
     * - 支持本地文件系统、HDFS、S3 等
     * - 支持多种文件格式
     * - 支持流式读取（监控新文件）
     */
    public void demonstrateFileSource(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== File Source 连接器演示 ==========");

        // ==================== 文本文件 Source ====================
        FileSource<String> textSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("/tmp/flink-input")
                )
                // 持续监控目录中的新文件
                .monitorContinuously(Duration.ofSeconds(10))
                .build();

        DataStream<String> textStream = env.fromSource(
                textSource,
                WatermarkStrategy.noWatermarks(),
                "Text File Source"
        );

        textStream.print("文件内容");

        // ==================== CSV 文件 Source ====================
        /*
         * 读取 CSV 文件示例
         *
         * FileSource<SensorReading> csvSource = FileSource
         *         .forRecordStreamFormat(
         *                 CsvReaderFormat.forPojo(SensorReading.class),
         *                 new Path("/tmp/sensors.csv")
         *         )
         *         .build();
         */

        System.out.println("File Source 配置完成");
        System.out.println("  - Path: /tmp/flink-input");
        System.out.println("  - Monitor Interval: 10 seconds");
    }

    /**
     * 演示 File Sink 连接器
     *
     * File Sink 用于将数据写入文件系统：
     * - 支持多种文件格式
     * - 支持文件滚动策略
     * - 支持精确一次语义
     */
    public void demonstrateFileSink(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== File Sink 连接器演示 ==========");

        // 创建数据流
        DataStream<String> dataStream = createStringSource(env);

        // ==================== File Sink 配置 ====================
        FileSink<String> fileSink = FileSink
                // 输出路径和编码器
                .forRowFormat(
                        new Path("/tmp/flink-output"),
                        new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8")
                )
                /*
                 * 文件滚动策略：
                 * - 文件大小达到阈值
                 * - 文件打开时间达到阈值
                 * - 没有新数据的时间达到阈值
                 */
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(org.apache.flink.configuration.MemorySize.ofMebiBytes(128))  // 128 MB
                                .withRolloverInterval(Duration.ofMinutes(15))  // 15 分钟
                                .withInactivityInterval(Duration.ofMinutes(5))  // 5 分钟无数据
                                .build()
                )
                /*
                 * 桶分配器：决定文件如何分目录存储
                 * - DateTimeBucketAssigner: 按时间分桶
                 * - BasePathBucketAssigner: 不分桶，全部写入基础路径
                 * - 自定义 BucketAssigner
                 */
                .withBucketAssigner(new org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner<>())
                .build();

        // 写入文件系统
        dataStream.sinkTo(fileSink);

        System.out.println("File Sink 配置完成");
        System.out.println("  - Path: /tmp/flink-output");
        System.out.println("  - Max Part Size: 128 MB");
        System.out.println("  - Rollover Interval: 15 minutes");

        /*
         * 输出目录结构示例：
         * /tmp/flink-output/
         * ├── 2024-01-15--10/
         * │   ├── part-0-0.txt
         * │   └── part-0-1.txt
         * └── 2024-01-15--11/
         *     └── part-0-0.txt
         */
    }

    /**
     * 演示 DataGen 连接器（用于测试）
     *
     * DataGen 连接器用于生成测试数据：
     * - 支持多种数据类型
     * - 支持随机数据生成
     * - 支持自定义生成器
     */
    public void demonstrateDataGenConnector(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== DataGen 连接器演示 ==========");

        /*
         * DataGen 连接器主要通过 Table API 使用
         * 在 SQL 中定义：
         *
         * CREATE TABLE datagen_source (
         *     id INT,
         *     name STRING,
         *     score DOUBLE
         * ) WITH (
         *     'connector' = 'datagen',
         *     'rows-per-second' = '100',
         *     'fields.id.kind' = 'sequence',
         *     'fields.id.start' = '1',
         *     'fields.id.end' = '1000',
         *     'fields.name.length' = '10',
         *     'fields.score.min' = '0',
         *     'fields.score.max' = '100'
         * )
         */

        System.out.println("DataGen 连接器配置示例：");
        System.out.println("  'connector' = 'datagen'");
        System.out.println("  'rows-per-second' = '100'");
        System.out.println("  'fields.id.kind' = 'sequence'");
        System.out.println("  'fields.name.length' = '10'");
    }

    /**
     * 演示自定义 Source 连接器
     *
     * 当内置连接器不满足需求时，可以自定义 Source
     */
    public void demonstrateCustomSource(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 自定义 Source 演示 ==========");

        // 使用自定义 SourceFunction
        DataStream<SensorReading> customStream = env.addSource(new CustomSensorSource())
                .name("Custom Sensor Source");

        customStream.print("自定义数据源");

        /*
         * 自定义 Source 的实现方式：
         *
         * 1. SourceFunction（旧 API）
         *    - 简单易用
         *    - 不支持并行
         *
         * 2. RichSourceFunction
         *    - 支持 open/close 生命周期
         *    - 支持 RuntimeContext
         *
         * 3. ParallelSourceFunction
         *    - 支持并行读取
         *
         * 4. Source（新 API，推荐）
         *    - 更好的分离关注点
         *    - 支持有界/无界
         *    - 支持 watermark
         */
    }

    /**
     * 自定义传感器数据源
     */
    private static class CustomSensorSource implements SourceFunction<SensorReading> {
        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};

            while (running) {
                for (String sensorId : sensorIds) {
                    SensorReading reading = SensorReading.builder()
                            .sensorId(sensorId)
                            .timestamp(System.currentTimeMillis())
                            .temperature(20 + random.nextDouble() * 20)
                            .humidity(40 + random.nextDouble() * 40)
                            .location("room_" + random.nextInt(5))
                            .build();

                    // 发送数据
                    ctx.collect(reading);
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
     * 创建字符串数据源
     */
    private DataStream<String> createStringSource(StreamExecutionEnvironment env) {
        return env.addSource(new SourceFunction<String>() {
            private volatile boolean running = true;
            private int count = 0;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (running) {
                    ctx.collect("Message " + (++count) + " at " + System.currentTimeMillis());
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
    }

    /**
     * 创建订单数据源
     */
    private DataStream<Order> createOrderSource(StreamExecutionEnvironment env) {
        return env.addSource(new SourceFunction<Order>() {
            private volatile boolean running = true;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                String[] userIds = {"user_1", "user_2", "user_3"};
                String[] statuses = {"CREATED", "PAID", "SHIPPED"};

                while (running) {
                    Order order = Order.builder()
                            .orderId("ORDER_" + System.currentTimeMillis())
                            .userId(userIds[random.nextInt(userIds.length)])
                            .amount(BigDecimal.valueOf(random.nextDouble() * 1000))
                            .status(statuses[random.nextInt(statuses.length)])
                            .createTime(System.currentTimeMillis())
                            .build();

                    ctx.collect(order);
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
    }

    /**
     * 运行所有连接器演示
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink 连接器演示");
        System.out.println("=".repeat(60));

        // 演示自定义 Source（不需要外部依赖）
        demonstrateCustomSource(env);

        // 以下演示需要相应的外部系统支持
        // demonstrateKafkaSource(env);
        // demonstrateKafkaSink(env);
        // demonstrateJdbcSink(env);
        // demonstrateFileSource(env);
        // demonstrateFileSink(env);

        env.execute("Connector Demo");
    }

    /**
     * 异步运行连接器演示，返回 JobClient 用于作业控制
     *
     * @param env Flink 执行环境
     * @return JobClient 用于取消作业
     */
    public org.apache.flink.core.execution.JobClient runAllDemosAsync(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink 连接器演示");
        System.out.println("=".repeat(60));

        // 演示自定义 Source（不需要外部依赖）
        demonstrateCustomSource(env);

        // 使用 executeAsync 返回 JobClient
        return env.executeAsync("Connector Demo");
    }
}
