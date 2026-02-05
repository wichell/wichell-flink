package com.wichell.flink.controller;

import com.wichell.flink.service.FlinkJobService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink 演示作业 REST API 控制器
 *
 * 提供 REST 接口来触发和管理各种 Flink 演示作业
 *
 * @author wichell
 */
@RestController
@RequestMapping("/api/flink")
@RequiredArgsConstructor
public class FlinkDemoController {

    private final FlinkJobService flinkJobService;

    /**
     * 获取 API 帮助信息
     */
    @GetMapping
    public Map<String, Object> getApiInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("name", "Flink Demo API");
        info.put("version", "1.0.0");
        info.put("description", "Spring Boot + Apache Flink 核心功能演示");

        Map<String, String> endpoints = new HashMap<>();
        endpoints.put("GET /api/flink", "获取 API 信息");
        endpoints.put("GET /api/flink/jobs", "获取运行中的作业");
        endpoints.put("POST /api/flink/demo/datastream", "运行 DataStream 基础演示");
        endpoints.put("POST /api/flink/demo/window", "运行窗口操作演示");
        endpoints.put("POST /api/flink/demo/state", "运行状态管理演示");
        endpoints.put("POST /api/flink/demo/checkpoint", "运行检查点演示");
        endpoints.put("POST /api/flink/demo/tableapi", "运行 Table API 演示");
        endpoints.put("POST /api/flink/demo/cep", "运行 CEP 复杂事件处理演示");
        endpoints.put("POST /api/flink/demo/connector", "运行连接器演示");
        endpoints.put("DELETE /api/flink/demo/{jobName}", "停止指定作业");
        info.put("endpoints", endpoints);

        Map<String, String> features = new HashMap<>();
        features.put("DataStream API", "map, flatMap, filter, keyBy, reduce, union, connect 等基础操作");
        features.put("Window", "滚动窗口、滑动窗口、会话窗口、窗口函数");
        features.put("State", "ValueState, ListState, MapState, ReducingState, TTL, Timer");
        features.put("Checkpoint", "检查点配置、状态后端、Savepoint、状态恢复");
        features.put("Table API & SQL", "Table API 操作、Flink SQL、UDF");
        features.put("CEP", "模式匹配、复杂事件处理、超时检测");
        features.put("Connectors", "Kafka、JDBC、File System 等连接器");
        info.put("features", features);

        return info;
    }

    /**
     * 获取运行中的作业列表
     */
    @GetMapping("/jobs")
    public Map<String, String> getRunningJobs() {
        Map<String, String> result = new HashMap<>();
        result.put("status", "success");
        result.put("jobs", flinkJobService.getRunningJobs());
        return result;
    }

    // ==================== DataStream 演示 ====================

    /**
     * 运行 DataStream 基础演示
     *
     * 演示 Flink DataStream API 的核心操作：
     * - Map: 一对一转换
     * - FlatMap: 一对多转换
     * - Filter: 数据过滤
     * - KeyBy: 按键分区
     * - Reduce: 滚动聚合
     * - Union: 流合并
     * - Connect: 连接不同类型的流
     */
    @PostMapping("/demo/datastream")
    public Map<String, String> runDataStreamDemo() {
        return createResponse(flinkJobService.runDataStreamDemo());
    }

    // ==================== Window 演示 ====================

    /**
     * 运行窗口操作演示
     *
     * 演示 Flink 窗口操作：
     * - 滚动窗口 (Tumbling Window): 固定大小，不重叠
     * - 滑动窗口 (Sliding Window): 固定大小，有重叠
     * - 会话窗口 (Session Window): 动态大小，基于活动间隙
     * - 窗口函数: ReduceFunction, AggregateFunction, ProcessWindowFunction
     * - Watermark: 事件时间处理和延迟数据
     */
    @PostMapping("/demo/window")
    public Map<String, String> runWindowDemo() {
        return createResponse(flinkJobService.runWindowDemo());
    }

    // ==================== State 演示 ====================

    /**
     * 运行状态管理演示
     *
     * 演示 Flink 状态管理：
     * - ValueState: 单值状态
     * - ListState: 列表状态
     * - MapState: 映射状态
     * - ReducingState: 聚合状态
     * - 状态 TTL: 状态过期配置
     * - Timer: 定时器与状态结合
     */
    @PostMapping("/demo/state")
    public Map<String, String> runStateDemo() {
        return createResponse(flinkJobService.runStateDemo());
    }

    // ==================== Checkpoint 演示 ====================

    /**
     * 运行检查点演示
     *
     * 演示 Flink 容错机制：
     * - 检查点配置: 间隔、超时、模式
     * - 状态后端: HashMapStateBackend, RocksDBStateBackend
     * - Savepoint: 手动触发的检查点
     * - 状态恢复: 从检查点恢复作业
     */
    @PostMapping("/demo/checkpoint")
    public Map<String, String> runCheckpointDemo() {
        return createResponse(flinkJobService.runCheckpointDemo());
    }

    // ==================== Table API 演示 ====================

    /**
     * 运行 Table API 和 SQL 演示
     *
     * 演示 Flink Table API：
     * - Table API 操作: select, filter, groupBy, join
     * - Flink SQL: DDL, DML, 窗口查询
     * - DataStream 与 Table 转换
     * - 窗口 TVF: TUMBLE, HOP, SESSION, CUMULATE
     * - UDF: ScalarFunction, TableFunction, AggregateFunction
     */
    @PostMapping("/demo/tableapi")
    public Map<String, String> runTableApiDemo() {
        return createResponse(flinkJobService.runTableApiDemo());
    }

    // ==================== CEP 演示 ====================

    /**
     * 运行 CEP 复杂事件处理演示
     *
     * 演示 Flink CEP：
     * - 模式定义: begin, next, followedBy
     * - 量词: times, oneOrMore, optional
     * - 条件: SimpleCondition, IterativeCondition
     * - 超时处理: PatternTimeoutFunction
     * - 跳过策略: AfterMatchSkipStrategy
     */
    @PostMapping("/demo/cep")
    public Map<String, String> runCepDemo() {
        return createResponse(flinkJobService.runCepDemo());
    }

    // ==================== Connector 演示 ====================

    /**
     * 运行连接器演示
     *
     * 演示 Flink 连接器：
     * - Kafka Source/Sink: 消息队列连接
     * - JDBC Sink: 数据库写入
     * - File Source/Sink: 文件系统读写
     * - 自定义 Source: 实现 SourceFunction
     */
    @PostMapping("/demo/connector")
    public Map<String, String> runConnectorDemo() {
        return createResponse(flinkJobService.runConnectorDemo());
    }

    // ==================== 作业管理 ====================

    /**
     * 停止指定的演示作业
     *
     * @param jobName 作业名称
     */
    @DeleteMapping("/demo/{jobName}")
    public Map<String, String> stopDemo(@PathVariable String jobName) {
        return createResponse(flinkJobService.stopDemo(jobName));
    }

    /**
     * 创建统一的响应格式
     */
    private Map<String, String> createResponse(String message) {
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", message);
        response.put("note", "请查看控制台输出以获取详细结果");
        return response;
    }
}
