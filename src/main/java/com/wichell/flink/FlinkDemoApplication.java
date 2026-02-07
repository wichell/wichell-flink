package com.wichell.flink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot + Flink 演示项目主入口
 *
 * 本项目演示 Apache Flink 的核心功能，包括：
 * 1. DataStream API - 流处理基础操作
 * 2. Window 窗口操作 - 滚动窗口、滑动窗口、会话窗口
 * 3. State 状态管理 - 键控状态、算子状态
 * 4. Checkpoint 检查点 - 容错机制
 * 5. Table API & SQL - 声明式数据处理
 * 6. CEP 复杂事件处理 - 模式匹配
 * 7. Connectors 连接器 - Kafka、JDBC、文件系统
 *
 * @author wichell
 * @version 1.0.0
 */
@SpringBootApplication
public class FlinkDemoApplication {

    public static void main(String[] args) {
        // 启动 Spring Boot 应用
        // Flink 作业可以通过 REST API 或直接调用 Service 来触发
        SpringApplication.run(FlinkDemoApplication.class, args);

        System.out.println("===========================================");
        System.out.println("  Flink Demo Application Started!");
        System.out.println("  访问 http://localhost:8080 查看 API");
        System.out.println("  Flink Web UI 端口请查看日志中的 'Web frontend listening at' 信息");
        System.out.println("===========================================");
    }
}
