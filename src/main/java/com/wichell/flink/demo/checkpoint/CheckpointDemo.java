package com.wichell.flink.demo.checkpoint;

import com.wichell.flink.model.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Random;

/**
 * Flink 检查点（Checkpoint）详细演示
 *
 * 检查点是 Flink 容错机制的核心，确保在发生故障时能够恢复状态。
 *
 * ==================== 检查点原理 ====================
 *
 * 1. Barrier 机制
 *    - Flink 在数据流中插入 Barrier（屏障）
 *    - Barrier 将数据流分割为多个部分
 *    - 当所有输入的 Barrier 对齐后，触发状态快照
 *
 * 2. 异步快照
 *    - 状态快照异步执行，不阻塞数据处理
 *    - 确保低延迟
 *
 * 3. 精确一次语义（Exactly-Once）
 *    - 通过 Barrier 对齐保证
 *    - 每条数据只被处理一次
 *
 * ==================== 检查点配置 ====================
 *
 * 1. 检查点间隔
 *    - 太短：频繁快照，增加开销
 *    - 太长：故障恢复时重新处理的数据量大
 *    - 建议：1-10 分钟
 *
 * 2. 检查点超时
 *    - 超过时间未完成则取消
 *    - 防止检查点阻塞作业
 *
 * 3. 最小间隔
 *    - 两次检查点之间的最小时间
 *    - 防止检查点过于频繁
 *
 * 4. 并发检查点
 *    - 允许同时进行的检查点数量
 *    - 通常设为 1
 *
 * ==================== 状态后端 ====================
 *
 * 1. HashMapStateBackend
 *    - 状态存储在 JVM 堆内存
 *    - 适用于小状态（< 几 GB）
 *    - 检查点时进行完整快照
 *
 * 2. EmbeddedRocksDBStateBackend
 *    - 状态存储在 RocksDB（本地磁盘）
 *    - 适用于大状态（TB 级别）
 *    - 支持增量检查点
 *    - 内存占用更可控
 *
 * ==================== 状态恢复说明 ====================
 *
 * 重要：手动关闭进程后重启，状态不会自动恢复！
 *
 * 自动恢复场景：
 * - 作业内部 Task 失败，Flink 自动重启策略生效
 * - 此时会自动从最新检查点恢复
 *
 * 手动恢复场景（需要指定检查点路径）：
 * - 进程被 kill
 * - 计划性停止后重启
 * - 需要使用 flink run -s <checkpoint-path> 启动
 *
 * @author wichell
 */
@Component
public class CheckpointDemo {

    private static final String CHECKPOINT_DIR = "file:///tmp/flink-checkpoints";
    private static final String JOB_NAME = "Checkpoint Demo";

    /**
     * 查找最新的检查点目录
     *
     * @return 最新检查点路径，如果没有则返回 null
     */
    private String findLatestCheckpoint() {
        try {
            java.io.File checkpointDir = new java.io.File("/tmp/flink-checkpoints");
            if (!checkpointDir.exists()) {
                return null;
            }

            // 查找所有作业目录
            java.io.File[] jobDirs = checkpointDir.listFiles(java.io.File::isDirectory);
            if (jobDirs == null || jobDirs.length == 0) {
                return null;
            }

            // 找到最新的检查点
            java.io.File latestCheckpoint = null;
            long latestTime = 0;

            for (java.io.File jobDir : jobDirs) {
                java.io.File[] chkDirs = jobDir.listFiles(f -> f.isDirectory() && f.getName().startsWith("chk-"));
                if (chkDirs != null) {
                    for (java.io.File chkDir : chkDirs) {
                        if (chkDir.lastModified() > latestTime) {
                            latestTime = chkDir.lastModified();
                            latestCheckpoint = chkDir;
                        }
                    }
                }
            }

            if (latestCheckpoint != null) {
                return latestCheckpoint.getAbsolutePath();
            }
        } catch (Exception e) {
            System.out.println("查找检查点时出错: " + e.getMessage());
        }
        return null;
    }

    /**
     * 演示检查点的基本配置
     *
     * 展示如何配置检查点的各项参数
     */
    public void demonstrateCheckpointConfig(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 检查点配置演示 ==========");

        // ==================== 1. 启用检查点 ====================
        /*
         * 参数：检查点间隔（毫秒）
         * 设置为 10 秒执行一次检查点
         */
        env.enableCheckpointing(10000);

        // ==================== 2. 获取检查点配置对象 ====================
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // ==================== 3. 设置检查点模式 ====================
        /*
         * CheckpointingMode.EXACTLY_ONCE（默认）
         * - 精确一次语义
         * - 需要 Barrier 对齐
         * - 可能产生反压
         *
         * CheckpointingMode.AT_LEAST_ONCE
         * - 至少一次语义
         * - 不需要 Barrier 对齐
         * - 延迟更低，但可能有重复
         */
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // ==================== 4. 设置检查点超时 ====================
        /*
         * 检查点超时时间（毫秒）
         * 超过此时间未完成则取消当前检查点
         * 默认 10 分钟
         */
        checkpointConfig.setCheckpointTimeout(60000);  // 1 分钟

        // ==================== 5. 设置最小间隔 ====================
        /*
         * 两次检查点之间的最小间隔（毫秒）
         * 防止检查点过于频繁
         * 与检查点间隔配合使用
         */
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // ==================== 6. 设置最大并发检查点 ====================
        /*
         * 允许同时进行的检查点数量
         * 设置为 1 可以确保上一个检查点完成后再开始下一个
         */
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // ==================== 7. 设置检查点存储 ====================
        /*
         * 检查点数据存储位置
         * 支持：
         * - 本地文件系统：file:///path/to/checkpoints
         * - HDFS：hdfs://namenode:port/path/to/checkpoints
         * - S3：s3://bucket/path/to/checkpoints
         */
        checkpointConfig.setCheckpointStorage(
                new FileSystemCheckpointStorage(CHECKPOINT_DIR));

        // ==================== 8. 设置外部化检查点（关键！）====================
        /*
         * 作业取消或完成后的检查点处理策略
         *
         * RETAIN_ON_CANCELLATION：
         * - 取消作业时保留检查点
         * - 便于后续恢复作业（手动重启场景必须设置）
         *
         * DELETE_ON_CANCELLATION：
         * - 取消作业时删除检查点
         * - 节省存储空间
         *
         * NO_EXTERNALIZED_CHECKPOINTS：
         * - 不外部化检查点（默认）
         */
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ==================== 9. 设置容忍的检查点失败次数 ====================
        /*
         * 允许检查点连续失败的次数
         * 超过此次数则作业失败
         * 默认 0（不容忍失败）
         */
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // ==================== 10. 配置重启策略（关键！）====================
        /*
         * 配置作业失败后的重启策略
         * 只有配置了重启策略，作业内部失败时才会自动从检查点恢复
         */
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                        3,  // 最多重启 3 次
                        org.apache.flink.api.common.time.Time.seconds(10)  // 每次重启间隔 10 秒
                ));

        // ==================== 11. 启用非对齐检查点（可选）====================
        /*
         * 非对齐检查点（Unaligned Checkpoints）
         * - 不需要 Barrier 对齐
         * - 减少反压场景下的检查点时间
         * - 检查点数据可能更大
         *
         * 适用场景：
         * - 存在反压
         * - 对检查点时间敏感
         */
        // checkpointConfig.enableUnalignedCheckpoints();
        // checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30));

        System.out.println("检查点配置完成:");
        System.out.println("  - 间隔: 10 秒");
        System.out.println("  - 模式: EXACTLY_ONCE");
        System.out.println("  - 超时: 60 秒");
        System.out.println("  - 存储: " + CHECKPOINT_DIR);
        System.out.println("  - 外部化: RETAIN_ON_CANCELLATION (作业停止后保留检查点)");
        System.out.println("  - 重启策略: 固定延迟重启，最多 3 次，间隔 10 秒");
        System.out.println();
        System.out.println("【重要提示】");
        System.out.println("  - 作业内部失败: 会自动从检查点恢复状态");
        System.out.println("  - 手动停止/kill: 需要从检查点路径手动恢复");
        System.out.println("  - 检查点目录: " + CHECKPOINT_DIR);
    }

    /**
     * 演示 HashMapStateBackend
     *
     * HashMapStateBackend 是默认的状态后端：
     * - 状态存储在 TaskManager 的 JVM 堆内存中
     * - 检查点时将完整状态序列化到外部存储
     * - 适用于小状态场景（几 GB 以内）
     *
     * 优点：
     * - 访问速度快（内存访问）
     * - 配置简单
     *
     * 缺点：
     * - 受 JVM 堆大小限制
     * - GC 压力可能较大
     */
    public void demonstrateHashMapStateBackend(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== HashMapStateBackend 演示 ==========");

        // ==================== 配置 HashMapStateBackend ====================
        HashMapStateBackend hashMapBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapBackend);

        // 配置检查点存储
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints/hashmap"));

        // 启用检查点
        env.enableCheckpointing(5000);

        System.out.println("HashMapStateBackend 配置完成");
        System.out.println("  - 状态存储: JVM 堆内存");
        System.out.println("  - 检查点存储: file:///tmp/flink-checkpoints/hashmap");

        // 创建演示作业
        createDemoJob(env, "HashMapBackend");
    }

    /**
     * 演示 RocksDBStateBackend
     *
     * EmbeddedRocksDBStateBackend 适用于大状态场景：
     * - 状态存储在 RocksDB（嵌入式 KV 数据库）
     * - RocksDB 数据存储在本地磁盘
     * - 支持增量检查点
     *
     * 优点：
     * - 支持超大状态（TB 级别）
     * - 内存使用可控
     * - 支持增量检查点
     *
     * 缺点：
     * - 访问速度相对较慢（磁盘访问）
     * - 需要额外的序列化/反序列化开销
     */
    public void demonstrateRocksDBStateBackend(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== RocksDBStateBackend 演示 ==========");

        // ==================== 配置 RocksDBStateBackend ====================
        /*
         * EmbeddedRocksDBStateBackend 配置选项：
         * - enableIncrementalCheckpointing: 启用增量检查点
         */
        EmbeddedRocksDBStateBackend rocksDBBackend = new EmbeddedRocksDBStateBackend(true);

        // 设置 RocksDB 本地存储路径（可选）
        // rocksDBBackend.setDbStoragePath("/tmp/rocksdb");

        env.setStateBackend(rocksDBBackend);

        // 配置检查点存储
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints/rocksdb"));

        // 启用检查点
        env.enableCheckpointing(5000);

        System.out.println("RocksDBStateBackend 配置完成");
        System.out.println("  - 状态存储: RocksDB (本地磁盘)");
        System.out.println("  - 增量检查点: 启用");
        System.out.println("  - 检查点存储: file:///tmp/flink-checkpoints/rocksdb");

        // 创建演示作业
        createDemoJob(env, "RocksDBBackend");
    }

    /**
     * 演示 Savepoint 的使用
     *
     * Savepoint 与 Checkpoint 的区别：
     *
     * Checkpoint：
     * - 自动触发
     * - 用于故障恢复
     * - 可以配置保留策略
     * - 使用状态后端的原生格式
     *
     * Savepoint：
     * - 手动触发
     * - 用于有计划的停止/升级
     * - 始终保留
     * - 使用统一的格式，可以跨版本恢复
     *
     * Savepoint 使用场景：
     * - 应用程序升级
     * - Flink 版本升级
     * - 作业迁移
     * - A/B 测试
     */
    public void demonstrateSavepoint(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== Savepoint 演示 ==========");

        /*
         * Savepoint 操作命令（使用 Flink CLI）：
         *
         * 1. 触发 Savepoint：
         *    flink savepoint <jobId> [targetDirectory]
         *
         * 2. 取消作业并创建 Savepoint：
         *    flink cancel -s [targetDirectory] <jobId>
         *
         * 3. 从 Savepoint 恢复作业：
         *    flink run -s <savepointPath> <jarFile>
         *
         * 4. 从 Savepoint 恢复并允许跳过不存在的状态：
         *    flink run -s <savepointPath> --allowNonRestoredState <jarFile>
         */

        System.out.println("Savepoint 相关命令：");
        System.out.println("  1. 触发 Savepoint:");
        System.out.println("     flink savepoint <jobId> /path/to/savepoints");
        System.out.println();
        System.out.println("  2. 取消作业并创建 Savepoint:");
        System.out.println("     flink cancel -s /path/to/savepoints <jobId>");
        System.out.println();
        System.out.println("  3. 从 Savepoint 恢复:");
        System.out.println("     flink run -s /path/to/savepoint app.jar");
        System.out.println();
        System.out.println("  4. REST API 触发 Savepoint:");
        System.out.println("     POST /jobs/:jobid/savepoints");
        System.out.println("     Body: {\"target-directory\": \"/path/to/savepoints\"}");

        // 配置用于 Savepoint 恢复的检查点
        env.enableCheckpointing(10000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    /**
     * 演示状态恢复机制
     *
     * Flink 从检查点/Savepoint 恢复的过程：
     *
     * 1. 读取最新的有效检查点
     * 2. 根据算子 ID 和状态名称匹配状态
     * 3. 将状态分发到各个 TaskManager
     * 4. 恢复数据源的位置（如 Kafka offset）
     * 5. 继续处理数据
     *
     * 状态匹配规则：
     * - 算子 ID：uid("operator-id")
     * - 状态名称：ValueStateDescriptor 中的名称
     */
    public void demonstrateStateRecovery(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== 状态恢复演示 ==========");

        DataStream<SensorReading> sensorStream = createSensorSource(env);

        // ==================== 为算子设置 UID ====================
        /*
         * 重要：为有状态的算子设置 UID
         *
         * UID 用于状态恢复时的匹配：
         * - 如果不设置 UID，Flink 会自动生成
         * - 自动生成的 UID 可能在代码变更后失效
         * - 建议：始终为有状态的算子显式设置 UID
         */
        DataStream<String> result = sensorStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new StatefulTemperatureProcessor())
                // 为有状态的算子设置 UID
                .uid("temperature-processor")
                .name("温度处理器");  // 设置算子名称（用于 UI 显示）

        result.print().uid("print-sink");

        System.out.println("已为所有有状态算子设置 UID");
        System.out.println("这确保了从检查点/Savepoint 恢复时状态能正确匹配");
    }

    /**
     * 有状态的温度处理器
     * 演示状态恢复场景
     */
    private static class StatefulTemperatureProcessor
            extends RichFlatMapFunction<SensorReading, String> {

        // 这些状态会被自动恢复
        private ValueState<Double> lastTempState;
        private ValueState<Long> countState;

        // 使用静态变量记录是否已触发过故障（JVM 级别，重启后保留）
        private static volatile boolean hasTriggeredFailure = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态描述符中的名称用于状态恢复时的匹配
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-temp", Double.class));
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void flatMap(SensorReading reading, Collector<String> out) throws Exception {
            // 获取并更新计数
            Long count = countState.value();
            count = (count == null) ? 1L : count + 1;
            countState.update(count);

            // 获取上次温度
            Double lastTemp = lastTempState.value();
            lastTempState.update(reading.getTemperature());

            if (lastTemp != null) {
                double diff = reading.getTemperature() - lastTemp;
                out.collect(String.format(
                        "[第%d条] %s: 温度 %.2f°C (变化: %+.2f°C)",
                        count, reading.getSensorId(), reading.getTemperature(), diff
                ));
            } else {
                out.collect(String.format(
                        "[第%d条] %s: 首条数据，温度 %.2f°C",
                        count, reading.getSensorId(), reading.getTemperature()
                ));
            }

            // ==================== 模拟故障：计数到 9 时触发（仅一次）====================
            // 使用静态变量确保整个 JVM 生命周期内只触发一次
            // 这样 Flink 重启后不会再次触发，可以观察到状态恢复效果
            //if (count == 9 && !hasTriggeredFailure) {
            //    hasTriggeredFailure = true;
            //    System.out.println("\n" + "=".repeat(50));
            //    System.out.println("⚠️  模拟故障触发！");
            //    System.out.println("    当前计数: " + count);
            //    System.out.println("    传感器: " + reading.getSensorId());
            //    System.out.println("    Flink 将自动重启并从检查点恢复状态");
            //    System.out.println("    恢复后计数应该从检查点保存的值继续，而不是从 1 开始");
            //    System.out.println("=".repeat(50) + "\n");
            //    throw new RuntimeException("模拟故障：计数达到 9，测试检查点恢复");
            //}
        }

        /**
         * 重置故障触发标志（用于下次演示）
         */
        public static void resetFailureFlag() {
            hasTriggeredFailure = false;
        }
    }

    /**
     * 创建演示作业
     */
    private void createDemoJob(StreamExecutionEnvironment env, String jobName) throws Exception {
        DataStream<SensorReading> sensorStream = createSensorSource(env);

        sensorStream
                .keyBy(SensorReading::getSensorId)
                .flatMap(new StatefulTemperatureProcessor())
                .print(jobName);
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

            while (running) {
                for (String sensorId : sensorIds) {
                    ctx.collect(SensorReading.builder()
                            .sensorId(sensorId)
                            .timestamp(System.currentTimeMillis())
                            .temperature(20 + random.nextDouble() * 20)
                            .build());
                }
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 运行检查点演示
     */
    public void runDemo(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink 检查点演示");
        System.out.println("=".repeat(60));

        demonstrateCheckpointConfig(env);
        demonstrateStateRecovery(env);

        env.execute("Checkpoint Demo");
    }

    /**
     * 异步运行检查点演示，返回 JobClient 用于作业控制
     */
    public org.apache.flink.core.execution.JobClient runDemoAsync(StreamExecutionEnvironment env) throws Exception {
        return runDemoAsync(env, false);
    }

    /**
     * 异步运行检查点演示，支持从检查点恢复
     *
     * @param env             执行环境
     * @param restoreFromCheckpoint 是否尝试从检查点恢复
     * @return JobClient
     */
    public org.apache.flink.core.execution.JobClient runDemoAsync(StreamExecutionEnvironment env, boolean restoreFromCheckpoint) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink 检查点演示");
        System.out.println("=".repeat(60));

        // 尝试从检查点恢复
        if (restoreFromCheckpoint) {
            String latestCheckpoint = findLatestCheckpoint();
            if (latestCheckpoint != null) {
                System.out.println("\n✅ 找到检查点，将从以下位置恢复:");
                System.out.println("   " + latestCheckpoint);
                System.out.println();

                // 重置故障标志，允许观察恢复后的状态
                StatefulTemperatureProcessor.resetFailureFlag();

                // 配置从检查点恢复
                org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
                config.setString("execution.savepoint.path", latestCheckpoint);

                // 使用配置创建新的环境
                env = StreamExecutionEnvironment.getExecutionEnvironment(config);
                env.setParallelism(2);
            } else {
                System.out.println("\n⚠️ 未找到检查点，将从头开始运行");
                System.out.println("   检查点目录: " + CHECKPOINT_DIR);
                System.out.println();
            }
        }

        demonstrateCheckpointConfig(env);
        demonstrateStateRecovery(env);

        return env.executeAsync("Checkpoint Demo");
    }

    /**
     * 清理检查点目录
     */
    public void cleanCheckpoints() {
        try {
            java.io.File checkpointDir = new java.io.File("/tmp/flink-checkpoints");
            if (checkpointDir.exists()) {
                deleteDirectory(checkpointDir);
                System.out.println("✅ 检查点目录已清理: " + checkpointDir.getAbsolutePath());
            }
        } catch (Exception e) {
            System.out.println("清理检查点时出错: " + e.getMessage());
        }
    }

    private void deleteDirectory(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
}
