package com.wichell.flink.service;

import com.wichell.flink.demo.cdc.MysqlCdcDemo;
import com.wichell.flink.demo.cep.CepDemo;
import com.wichell.flink.demo.checkpoint.CheckpointDemo;
import com.wichell.flink.demo.connector.ConnectorDemo;
import com.wichell.flink.demo.datastream.DataStreamBasicDemo;
import com.wichell.flink.demo.state.StateDemo;
import com.wichell.flink.demo.tableapi.TableApiDemo;
import com.wichell.flink.demo.window.WindowDemo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
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
    private final MysqlCdcDemo mysqlCdcDemo;

    // 存储正在运行的作业 Future
    private final ConcurrentHashMap<String, CompletableFuture<Void>> runningJobs = new ConcurrentHashMap<>();

    // 存储 Flink JobClient，用于真正停止作业
    private final ConcurrentHashMap<String, JobClient> jobClients = new ConcurrentHashMap<>();

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
        return runDemoWithJobClient("datastream-demo", env -> {
            log.info("启动 DataStream 基础演示...");
            try {
                return dataStreamDemo.runAllDemosAsync(env);
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
        return runDemoWithJobClient("window-demo", env -> {
            log.info("启动窗口操作演示...");
            try {
                return windowDemo.runAllDemosAsync(env);
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
        return runDemoWithJobClient("state-demo", env -> {
            log.info("启动状态管理演示...");
            try {
                return stateDemo.runAllDemosAsync(env);
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
        return runDemoWithJobClient("checkpoint-demo", env -> {
            log.info("启动检查点演示...");
            try {
                return checkpointDemo.runDemoAsync(env);
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
        return runDemoWithJobClient("tableapi-demo", env -> {
            log.info("启动 Table API 演示...");
            try {
                return tableApiDemo.runAllDemosAsync(env);
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
        return runDemoWithJobClient("cep-demo", env -> {
            log.info("启动 CEP 演示...");
            try {
                return cepDemo.runAllDemosAsync(env);
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
        return runDemoWithJobClient("connector-demo", env -> {
            log.info("启动连接器演示...");
            try {
                return connectorDemo.runAllDemosAsync(env);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 运行 MySQL CDC 演示
     *
     * 演示内容：
     * - 基于 Binlog 的变更数据捕获
     * - 全量 + 增量同步
     * - INSERT/UPDATE/DELETE 事件监听
     *
     * 注意：需要 MySQL 开启 Binlog 并配置正确的权限
     */
    public String runMysqlCdcDemo() {
        return runDemoWithJobClient("mysql-cdc-demo", env -> {
            log.info("启动 MySQL CDC 演示...");
            try {
                return mysqlCdcDemo.runDemoAsync(env, "sync");
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
        // 先尝试取消 Flink 作业
        JobClient jobClient = jobClients.get(jobName);
        if (jobClient != null) {
            try {
                log.info("正在取消 Flink 作业 {}...", jobName);
                jobClient.cancel().get(); // 等待作业真正取消
                log.info("Flink 作业 {} 已取消", jobName);
            } catch (Exception e) {
                log.warn("取消 Flink 作业 {} 时出现异常: {}", jobName, e.getMessage());
            } finally {
                jobClients.remove(jobName);
            }
        }

        // 然后取消 CompletableFuture
        CompletableFuture<Void> job = runningJobs.get(jobName);
        if (job != null) {
            job.cancel(true);
            runningJobs.remove(jobName);
            log.info("作业 {} 已停止", jobName);
            return "作业 " + jobName + " 已停止";
        }

        if (jobClient != null) {
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
                log.info("作业 {} 开始执行...", jobName);
                demoRunner.run();
                log.info("作业 {} 执行完成", jobName);
            } catch (Exception e) {
                // 如果是被取消的，不打印错误
                if (e.getCause() instanceof org.apache.flink.runtime.client.JobCancellationException) {
                    log.info("作业 {} 已被取消", jobName);
                } else {
                    log.error("作业 {} 执行失败: {}", jobName, e.getMessage(), e);
                    Throwable cause = e.getCause();
                    while (cause != null) {
                        log.error("  Caused by: {}", cause.getMessage());
                        cause = cause.getCause();
                    }
                }
            } finally {
                runningJobs.remove(jobName);
                jobClients.remove(jobName);
            }
        });

        runningJobs.put(jobName, future);
        return "作业 " + jobName + " 已启动，请查看控制台输出";
    }

    /**
     * 运行演示作业（支持保存 JobClient）
     */
    private String runDemoWithJobClient(String jobName, java.util.function.Function<StreamExecutionEnvironment, org.apache.flink.core.execution.JobClient> demoRunner) {
        if (runningJobs.containsKey(jobName)) {
            return "作业 " + jobName + " 已在运行中";
        }

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                log.info("作业 {} 开始执行...", jobName);
                StreamExecutionEnvironment newEnv = createNewEnv();
                JobClient client = demoRunner.apply(newEnv);
                if (client != null) {
                    jobClients.put(jobName, client);
                    // 等待作业完成
                    client.getJobExecutionResult().get();
                }
                log.info("作业 {} 执行完成", jobName);
            } catch (Exception e) {
                if (e.getCause() instanceof org.apache.flink.runtime.client.JobCancellationException
                    || e instanceof java.util.concurrent.CancellationException) {
                    log.info("作业 {} 已被取消", jobName);
                } else {
                    log.error("作业 {} 执行失败: {}", jobName, e.getMessage(), e);
                }
            } finally {
                runningJobs.remove(jobName);
                jobClients.remove(jobName);
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
