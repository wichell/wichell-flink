package com.wichell.flink.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;

/**
 * Flink 执行环境配置类
 *
 * 提供 StreamExecutionEnvironment 的统一配置，包括：
 * - 并行度设置
 * - 检查点配置
 * - 状态后端配置
 * - 重启策略配置
 *
 * @author wichell
 */
@org.springframework.context.annotation.Configuration
public class FlinkConfig {

    // ==================== 从配置文件读取参数 ====================

    @Value("${flink.execution.parallelism:4}")
    private int parallelism;

    @Value("${flink.checkpoint.enabled:true}")
    private boolean checkpointEnabled;

    @Value("${flink.checkpoint.interval:60000}")
    private long checkpointInterval;

    @Value("${flink.checkpoint.timeout:600000}")
    private long checkpointTimeout;

    @Value("${flink.checkpoint.storage-path:file:///tmp/flink-checkpoints}")
    private String checkpointStoragePath;

    @Value("${flink.checkpoint.min-pause:500}")
    private long minPauseBetweenCheckpoints;

    @Value("${flink.checkpoint.max-concurrent:1}")
    private int maxConcurrentCheckpoints;

    @Value("${flink.restart-strategy.attempts:3}")
    private int restartAttempts;

    @Value("${flink.restart-strategy.delay:10}")
    private int restartDelay;

    /**
     * 创建并配置 Flink 流执行环境
     *
     * StreamExecutionEnvironment 是 Flink 流处理程序的入口点
     * 所有的 DataStream 操作都需要通过它来创建和执行
     *
     * @return 配置好的 StreamExecutionEnvironment
     */
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        // ==================== 1. 创建执行环境 ====================
        /*
         * 创建带 Web UI 的本地执行环境
         * - 可以在 http://localhost:8081 查看 Flink Dashboard
         * - 用于本地开发和调试
         *
         * 生产环境通常使用:
         * StreamExecutionEnvironment.getExecutionEnvironment()
         * 它会自动检测运行环境（本地/集群）
         */
        Configuration config = new Configuration();
        // 设置 Flink Web UI 端口
        config.setInteger(RestOptions.PORT, 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(config);

        // ==================== 2. 设置并行度 ====================
        /*
         * 并行度（Parallelism）决定了任务的并发执行程度
         * - 较高的并行度可以提高吞吐量
         * - 但也会消耗更多资源
         * - 通常设置为可用 CPU 核心数
         */
        env.setParallelism(parallelism);

        // ==================== 3. 配置检查点 Checkpoint ====================
        /*
         * 检查点是 Flink 容错机制的核心
         * 它会定期将状态快照保存到持久化存储中
         * 当发生故障时，可以从最近的检查点恢复
         */
        if (checkpointEnabled) {
            // 启用检查点，设置间隔时间
            env.enableCheckpointing(checkpointInterval);

            CheckpointConfig checkpointConfig = env.getCheckpointConfig();

            /*
             * 检查点模式：
             * - EXACTLY_ONCE: 精确一次语义，保证数据不丢失不重复（默认）
             * - AT_LEAST_ONCE: 至少一次语义，可能有重复但延迟更低
             */
            checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

            // 检查点超时时间 - 超过此时间未完成则取消
            checkpointConfig.setCheckpointTimeout(checkpointTimeout);

            // 两次检查点之间的最小间隔
            checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);

            // 允许同时进行的最大检查点数量
            checkpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);

            // 设置检查点存储位置
            checkpointConfig.setCheckpointStorage(
                    new FileSystemCheckpointStorage(checkpointStoragePath));

            /*
             * 外部化检查点配置：
             * - RETAIN_ON_CANCELLATION: 取消作业时保留检查点
             * - DELETE_ON_CANCELLATION: 取消作业时删除检查点
             *
             * 保留检查点便于后续恢复作业
             */
            checkpointConfig.setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            /*
             * 启用非对齐检查点（Unaligned Checkpoints）
             * - 可以减少反压场景下的检查点时间
             * - 适用于有反压的场景
             */
            // checkpointConfig.enableUnalignedCheckpoints();
        }

        // ==================== 4. 配置状态后端 ====================
        /*
         * 状态后端决定了状态如何存储
         *
         * HashMapStateBackend（默认）:
         * - 状态存储在 JVM 堆内存中
         * - 适用于小状态场景
         * - 检查点时会进行完整快照
         *
         * EmbeddedRocksDBStateBackend:
         * - 状态存储在 RocksDB 中（本地磁盘）
         * - 适用于大状态场景
         * - 支持增量检查点
         */
        env.setStateBackend(new HashMapStateBackend());

        // 如果需要使用 RocksDB 状态后端:
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // ==================== 5. 配置重启策略 ====================
        /*
         * 重启策略定义了作业失败时的恢复行为
         *
         * 常用策略：
         * - fixedDelayRestart: 固定延迟重启
         * - failureRateRestart: 基于失败率的重启
         * - noRestart: 不重启
         */
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        restartAttempts,  // 最大重启次数
                        Time.of(restartDelay, TimeUnit.SECONDS)  // 重启间隔
                )
        );

        /*
         * 基于失败率的重启策略示例：
         * 在指定时间间隔内，如果失败次数超过阈值，则停止重启
         *
         * env.setRestartStrategy(
         *     RestartStrategies.failureRateRestart(
         *         3,  // 时间间隔内最大失败次数
         *         Time.of(5, TimeUnit.MINUTES),  // 时间间隔
         *         Time.of(10, TimeUnit.SECONDS)  // 重启延迟
         *     )
         * );
         */

        return env;
    }
}
