package com.wichell.flink.service;

import com.wichell.flink.demo.cep.CepDemo;
import com.wichell.flink.demo.checkpoint.CheckpointDemo;
import com.wichell.flink.demo.connector.ConnectorDemo;
import com.wichell.flink.demo.datastream.DataStreamBasicDemo;
import com.wichell.flink.demo.state.StateDemo;
import com.wichell.flink.demo.tableapi.TableApiDemo;
import com.wichell.flink.demo.window.WindowDemo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flink 作业管理服务
 *
 * 提供 Flink 作业的启动、停止、状态查询等功能
 * 通过 REST API 触发各种 Flink 演示
 *
 * @author wichell
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FlinkJobService {

    private final StreamExecutionEnvironment env;
    private final DataStreamBasicDemo dataStreamDemo;
    private final WindowDemo windowDemo;
    private final StateDemo stateDemo;
    private final CheckpointDemo checkpointDemo;
    private final TableApiDemo tableApiDemo;
    private final CepDemo cepDemo;
    private final ConnectorDemo connectorDemo;

    // 存储正在运行的作业
    private final ConcurrentHashMap<String, CompletableFuture<Void>> runningJobs = new ConcurrentHashMap<>();

    /**
     * 运行 DataStream 基础演示
     *
     * 演示内容：
     * - Map, FlatMap, Filter 转换
     * - KeyBy 分区
     * - Reduce 聚合
     * - Union, Connect 多流操作
     */
    public String runDataStreamDemo() {
        return runDemo("datastream-demo", () -> {
            log.info("启动 DataStream 基础演示...");
            try {
                dataStreamDemo.runAllDemos(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行窗口操作演示
     *
     * 演示内容：
     * - 滚动窗口 (Tumbling Window)
     * - 滑动窗口 (Sliding Window)
     * - 会话窗口 (Session Window)
     * - 窗口函数 (ReduceFunction, AggregateFunction, ProcessWindowFunction)
     * - Watermark 和延迟数据处理
     */
    public String runWindowDemo() {
        return runDemo("window-demo", () -> {
            log.info("启动窗口操作演示...");
            try {
                windowDemo.runAllDemos(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行状态管理演示
     *
     * 演示内容：
     * - ValueState
     * - ListState
     * - MapState
     * - ReducingState
     * - 状态 TTL
     * - 定时器与状态
     */
    public String runStateDemo() {
        return runDemo("state-demo", () -> {
            log.info("启动状态管理演示...");
            try {
                stateDemo.runAllDemos(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行检查点演示
     *
     * 演示内容：
     * - 检查点配置
     * - 状态后端 (HashMapStateBackend, RocksDBStateBackend)
     * - Savepoint
     * - 状态恢复
     */
    public String runCheckpointDemo() {
        return runDemo("checkpoint-demo", () -> {
            log.info("启动检查点演示...");
            try {
                checkpointDemo.runDemo(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行 Table API 演示
     *
     * 演示内容：
     * - Table API 基本操作
     * - Flink SQL
     * - DataStream 与 Table 转换
     * - 窗口 TVF
     * - 用户自定义函数 (UDF)
     */
    public String runTableApiDemo() {
        return runDemo("tableapi-demo", () -> {
            log.info("启动 Table API 演示...");
            try {
                tableApiDemo.runAllDemos(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行 CEP 复杂事件处理演示
     *
     * 演示内容：
     * - 简单模式匹配
     * - 复杂模式
     * - 超时处理
     * - 迭代条件
     * - 量词
     */
    public String runCepDemo() {
        return runDemo("cep-demo", () -> {
            log.info("启动 CEP 演示...");
            try {
                cepDemo.runAllDemos(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行连接器演示
     *
     * 演示内容：
     * - Kafka Source/Sink
     * - JDBC Sink
     * - File Source/Sink
     * - 自定义 Source
     */
    public String runConnectorDemo() {
        return runDemo("connector-demo", () -> {
            log.info("启动连接器演示...");
            try {
                connectorDemo.runAllDemos(createNewEnv());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 停止指定的演示作业
     *
     * @param jobName 作业名称
     * @return 停止结果
     */
    public String stopDemo(String jobName) {
        CompletableFuture<Void> job = runningJobs.get(jobName);
        if (job != null) {
            job.cancel(true);
            runningJobs.remove(jobName);
            log.info("作业 {} 已停止", jobName);
            return "作业 " + jobName + " 已停止";
        }
        return "作业 " + jobName + " 未运行";
    }

    /**
     * 获取所有运行中的作业
     *
     * @return 运行中的作业列表
     */
    public String getRunningJobs() {
        if (runningJobs.isEmpty()) {
            return "当前没有运行中的作业";
        }
        StringBuilder sb = new StringBuilder("运行中的作业:\n");
        runningJobs.keySet().forEach(job -> sb.append("  - ").append(job).append("\n"));
        return sb.toString();
    }

    /**
     * 运行演示作业的通用方法
     */
    private String runDemo(String jobName, Runnable demoRunner) {
        if (runningJobs.containsKey(jobName)) {
            return "作业 " + jobName + " 已在运行中";
        }

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                demoRunner.run();
            } catch (Exception e) {
                log.error("作业 {} 执行失败", jobName, e);
            } finally {
                runningJobs.remove(jobName);
            }
        });

        runningJobs.put(jobName, future);
        return "作业 " + jobName + " 已启动，请查看控制台输出";
    }

    /**
     * 创建新的执行环境
     * 每个作业使用独立的执行环境
     */
    private StreamExecutionEnvironment createNewEnv() {
        StreamExecutionEnvironment newEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        newEnv.setParallelism(2);
        return newEnv;
    }
}
