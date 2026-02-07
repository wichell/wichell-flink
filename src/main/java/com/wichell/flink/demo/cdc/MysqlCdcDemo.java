package com.wichell.flink.demo.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Flink CDC MySQL 演示
 *
 * 通过 MySQL Binlog 捕获数据库变更事件（Change Data Capture）
 *
 * ==================== CDC 核心概念 ====================
 *
 * 1. 什么是 CDC？
 *    - Change Data Capture（变更数据捕获）
 *    - 实时捕获数据库的增删改操作
 *    - 基于数据库日志（如 MySQL Binlog）实现
 *
 * 2. Flink CDC 的优势：
 *    - 精确一次语义（Exactly-Once）
 *    - 无锁读取，不影响数据库性能
 *    - 支持全量 + 增量一体化同步
 *    - 支持断点续传
 *
 * 3. 启动模式（StartupOptions）：
 *    - initial(): 先全量读取，再增量同步（默认）
 *    - earliest(): 从最早的 Binlog 开始读取
 *    - latest(): 只读取最新的变更
 *    - specificOffset(): 从指定位置开始
 *    - timestamp(): 从指定时间戳开始
 *
 * ==================== MySQL 配置要求 ====================
 *
 * 1. 开启 Binlog：
 *    [mysqld]
 *    server-id = 1
 *    log_bin = mysql-bin
 *    binlog_format = ROW
 *    binlog_row_image = FULL
 *
 * 2. 创建 CDC 用户并授权：
 *    CREATE USER 'flink_cdc'@'%' IDENTIFIED BY 'flink_cdc_password';
 *    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_cdc'@'%';
 *    FLUSH PRIVILEGES;
 *
 * @author wichell
 */
@Slf4j
@Component
public class MysqlCdcDemo {

    @Value("${flink.cdc.mysql.hostname:localhost}")
    private String hostname;

    @Value("${flink.cdc.mysql.port:3306}")
    private int port;

    @Value("${flink.cdc.mysql.username:root}")
    private String username;

    @Value("${flink.cdc.mysql.password:root}")
    private String password;

    @Value("${flink.cdc.mysql.database:test_db}")
    private String database;

    @Value("${flink.cdc.mysql.table:users}")
    private String table;

    /**
     * 演示基本的 MySQL CDC 功能
     *
     * 捕获指定表的所有变更事件，输出为 JSON 格式
     */
    public void demonstrateBasicCdc(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("MySQL CDC 基础演示");

        log.info("CDC 配置信息:");
        log.info("  MySQL 地址: {}:{}", hostname, port);
        log.info("  数据库: {}", database);
        log.info("  监听表: {}", table);

        // 创建 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                // 数据库连接配置
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                // 监听的数据库和表
                // 支持正则表达式，如 "db.*" 监听所有以 db 开头的数据库
                .databaseList(database)
                // 表名格式：database.table，支持正则
                .tableList(database + "." + table)
                // 反序列化器：将 Binlog 事件转为 JSON 字符串
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 启动模式：先全量读取现有数据，再增量捕获变更
                .startupOptions(StartupOptions.initial())
                // 服务器 ID，需要在 MySQL 集群中唯一
                .serverId("5400-5404")
                .build();

        // 从 CDC Source 创建数据流
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                // 设置并行度为 1，保证事件顺序
                .setParallelism(1)
                // 打印变更事件
                .print("CDC Event");

        log.info("MySQL CDC 作业已配置，等待启动...");
        log.info("提示: 对 {}.{} 表执行 INSERT/UPDATE/DELETE 操作将被捕获", database, table);
    }

    /**
     * 演示带过滤的 CDC
     *
     * 只捕获特定条件的变更事件
     */
    public void demonstrateCdcWithFilter(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("MySQL CDC 过滤演示");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(database)
                .tableList(database + "." + table)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest()) // 只监听新的变更
                .serverId("5405-5409")
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(1)
                // 过滤：只保留 INSERT 和 UPDATE 操作
                .filter(json -> {
                    // JSON 中的 op 字段表示操作类型：
                    // c = create (INSERT)
                    // u = update (UPDATE)
                    // d = delete (DELETE)
                    // r = read (全量读取阶段)
                    return json.contains("\"op\":\"c\"") || json.contains("\"op\":\"u\"");
                })
                .print("Filtered CDC Event");

        log.info("带过滤的 CDC 作业已配置");
    }

    /**
     * 演示多表 CDC
     *
     * 同时监听多个表的变更
     */
    public void demonstrateMultiTableCdc(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("MySQL CDC 多表演示");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                // 监听整个数据库
                .databaseList(database)
                // 使用正则表达式监听多个表
                // 例如：监听所有以 order_ 开头的表
                .tableList(database + ".*")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .serverId("5410-5414")
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Multi-Table CDC Source")
                .setParallelism(1)
                .print("Multi-Table CDC Event");

        log.info("多表 CDC 作业已配置，监听数据库 {} 下的所有表", database);
    }

    /**
     * 演示 CDC 数据同步到另一张表
     *
     * 捕获 users 表的变更，解析后写入 user_sync 表
     *
     * 目标表结构（需要提前创建）：
     * CREATE TABLE user_sync (
     *     id BIGINT PRIMARY KEY,
     *     name VARCHAR(100),
     *     email VARCHAR(100),
     *     operation VARCHAR(10),
     *     op_ts TIMESTAMP,
     *     sync_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     * );
     */
    public void demonstrateCdcToMysql(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("MySQL CDC 同步到 MySQL 演示");

        log.info("CDC 配置信息:");
        log.info("  源表: {}.{}", database, table);
        log.info("  目标表: {}.user_sync", database);

        log.info("正在创建 MySQL CDC Source...");

        // 创建 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(database)
                .tableList(database + "." + table)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .serverId("5415-5419")
                .build();

        log.info("MySQL CDC Source 创建成功");

        log.info("正在配置数据流...");

        // 从 CDC Source 创建数据流，解析 JSON 并转换为 UserSync 对象
        DataStream<UserSyncRecord> syncStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(1)
                .map(json -> {
                    // ObjectMapper 在 lambda 内部创建，避免序列化问题
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode root = mapper.readTree(json);

                    // 获取操作类型: c=create, u=update, d=delete, r=read
                    String op = root.path("op").asText();
                    String operation = switch (op) {
                        case "c" -> "INSERT";
                        case "u" -> "UPDATE";
                        case "d" -> "DELETE";
                        case "r" -> "READ";
                        default -> "UNKNOWN";
                    };

                    // 获取数据：对于 DELETE 使用 before，其他使用 after
                    JsonNode data = "d".equals(op) ? root.path("before") : root.path("after");

                    // 获取事件时间戳
                    long tsMs = root.path("ts_ms").asLong(System.currentTimeMillis());

                    return new UserSyncRecord(
                            data.path("id").asLong(),
                            data.path("name").asText(""),
                            data.path("email").asText(""),
                            operation,
                            new Timestamp(tsMs)
                    );
                });

        // 打印同步记录
        syncStream.print("Sync Record");

        // 构建 JDBC 连接 URL
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                hostname, port, database);

        // 写入 MySQL user_sync 表
        syncStream.addSink(JdbcSink.sink(
                // INSERT 或 UPDATE（使用 REPLACE INTO 实现 upsert）
                "REPLACE INTO user_sync (id, name, email, operation, op_ts, sync_ts) VALUES (?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setLong(1, record.id());
                    ps.setString(2, record.name());
                    ps.setString(3, record.email());
                    ps.setString(4, record.operation());
                    ps.setTimestamp(5, record.opTs());
                    ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        )).name("MySQL Sink");

        log.info("CDC 同步作业已配置");
        log.info("提示: 对 {}.{} 表的变更将同步到 {}.user_sync", database, table, database);
    }

    /**
     * 用于同步的用户记录（使用 POJO 而非 record，因为 Flink Kryo 不支持 Java Record）
     */
    public static class UserSyncRecord implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private long id;
        private String name;
        private String email;
        private String operation;
        private Timestamp opTs;

        public UserSyncRecord() {}

        public UserSyncRecord(long id, String name, String email, String operation, Timestamp opTs) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.operation = operation;
            this.opTs = opTs;
        }

        public long id() { return id; }
        public String name() { return name; }
        public String email() { return email; }
        public String operation() { return operation; }
        public Timestamp opTs() { return opTs; }

        public void setId(long id) { this.id = id; }
        public void setName(String name) { this.name = name; }
        public void setEmail(String email) { this.email = email; }
        public void setOperation(String operation) { this.operation = operation; }
        public void setOpTs(Timestamp opTs) { this.opTs = opTs; }

        @Override
        public String toString() {
            return "UserSyncRecord{id=" + id + ", name='" + name + "', email='" + email +
                   "', operation='" + operation + "', opTs=" + opTs + "}";
        }
    }

    /**
     * 运行指定的 CDC 演示
     *
     * @param env      Flink 执行环境
     * @param demoName 演示名称：basic, filter, multi-table
     */
    public void runDemo(StreamExecutionEnvironment env, String demoName) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("Flink CDC 演示 - " + demoName);

        switch (demoName.toLowerCase()) {
            case "basic":
                demonstrateBasicCdc(env);
                break;
            case "filter":
                demonstrateCdcWithFilter(env);
                break;
            case "multi-table":
                demonstrateMultiTableCdc(env);
                break;
            case "sync":
                demonstrateCdcToMysql(env);
                break;
            default:
                log.error("未知的演示名称: {}，可选值: basic, filter, multi-table, sync", demoName);
                return;
        }

        env.execute("MySQL CDC Demo - " + demoName);
    }

    /**
     * 运行默认的 CDC 演示（基础演示）
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        com.wichell.flink.util.FlinkUtils.printSeparator("Flink MySQL CDC 演示");
        runDemo(env, "sync");
    }
}