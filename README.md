# Wichell Flink Demo

> Spring Boot + Apache Flink 核心功能演示项目

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/projects/jdk/17/)
[![Flink](https://img.shields.io/badge/Flink-1.18.1-blue.svg)](https://flink.apache.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.1-green.svg)](https://spring.io/projects/spring-boot)

## 📖 项目简介

本项目是一个全面的 Apache Flink 学习演示项目，整合了 Spring Boot 框架，通过 REST API 触发各种 Flink 核心功能演示。项目包含详细的中文注释，适合 Flink 初学者和进阶开发者学习参考。

## ✨ 功能特性

| 模块 | 功能描述 |
|------|----------|
| **DataStream API** | map、flatMap、filter、keyBy、reduce、union、connect、RichFunction 等核心转换操作 |
| **Window 窗口** | 滚动窗口、滑动窗口、会话窗口、计数窗口、窗口函数 |
| **State 状态管理** | ValueState、ListState、MapState、ReducingState、状态 TTL、定时器 |
| **Checkpoint 检查点** | 检查点配置、HashMapStateBackend、RocksDBStateBackend、Savepoint、状态恢复 |
| **Table API & SQL** | Table API 操作、Flink SQL、DataStream/Table 转换、窗口 TVF、UDF |
| **CEP 复杂事件处理** | 模式匹配、量词、迭代条件、超时处理、跳过策略 |
| **Connectors 连接器** | Kafka、JDBC、File System、自定义 Source/Sink |

## 🏗️ 项目结构

```
wichell-flink/
├── pom.xml                                    # Maven 配置文件
├── README.md                                  # 项目说明文档
├── src/main/java/com/wichell/flink/
│   ├── FlinkDemoApplication.java              # Spring Boot 启动类
│   ├── config/
│   │   └── FlinkConfig.java                   # Flink 执行环境配置
│   ├── model/                                 # 数据模型
│   │   ├── SensorReading.java                 # 传感器数据模型
│   │   ├── Order.java                         # 订单数据模型
│   │   └── UserEvent.java                     # 用户事件模型
│   ├── demo/                                  # 核心功能演示
│   │   ├── datastream/
│   │   │   └── DataStreamBasicDemo.java       # DataStream API 演示
│   │   ├── window/
│   │   │   └── WindowDemo.java                # 窗口操作演示
│   │   ├── state/
│   │   │   └── StateDemo.java                 # 状态管理演示
│   │   ├── checkpoint/
│   │   │   └── CheckpointDemo.java            # 检查点演示
│   │   ├── tableapi/
│   │   │   └── TableApiDemo.java              # Table API & SQL 演示
│   │   ├── cep/
│   │   │   └── CepDemo.java                   # CEP 复杂事件处理演示
│   │   └── connector/
│   │       └── ConnectorDemo.java             # 连接器演示
│   ├── service/
│   │   └── FlinkJobService.java               # 作业管理服务
│   ├── controller/
│   │   └── FlinkDemoController.java           # REST API 控制器
│   └── util/
│       └── FlinkUtils.java                    # 工具类
└── src/main/resources/
    └── application.yml                        # 应用配置文件
```

## 🚀 快速开始

### 环境要求

- **JDK 17+**
- **Maven 3.6+**
- **IDE**：推荐 IntelliJ IDEA

### 启动项目

```bash
# 1. 克隆项目
git clone <repository-url>
cd wichell-flink

# 2. 编译项目
mvn clean compile

# 3. 启动应用
mvn spring-boot:run
```

### 访问服务

- **Spring Boot 应用**：http://localhost:8888
- **Flink Web UI**：http://localhost:8081（运行作业后可用）

## 📡 REST API

### 获取 API 信息

```bash
curl http://localhost:8888/api/flink
```

### 获取运行中的作业

```bash
curl http://localhost:8888/api/flink/jobs
```

### 运行演示作业

| 演示 | 命令 |
|------|------|
| DataStream 基础 | `curl -X POST http://localhost:8888/api/flink/demo/datastream` |
| 窗口操作 | `curl -X POST http://localhost:8888/api/flink/demo/window` |
| 状态管理 | `curl -X POST http://localhost:8888/api/flink/demo/state` |
| 检查点 | `curl -X POST http://localhost:8888/api/flink/demo/checkpoint` |
| Table API | `curl -X POST http://localhost:8888/api/flink/demo/tableapi` |
| CEP | `curl -X POST http://localhost:8888/api/flink/demo/cep` |
| 连接器 | `curl -X POST http://localhost:8888/api/flink/demo/connector` |

### 停止作业

```bash
curl -X DELETE http://localhost:8888/api/flink/demo/{jobName}
```

## 📚 模块详解

### 1. DataStream API

**文件位置**：`demo/datastream/DataStreamBasicDemo.java`

演示 Flink DataStream API 的核心转换操作：

```java
// Map - 一对一转换
dataStream.map(reading -> reading.getTemperature());

// FlatMap - 一对多转换（WordCount 示例）
dataStream.flatMap((line, out) -> {
    for (String word : line.split("\\s+")) {
        out.collect(Tuple2.of(word, 1));
    }
});

// Filter - 过滤
dataStream.filter(reading -> reading.getTemperature() > 30.0);

// KeyBy + Reduce - 分组聚合
dataStream.keyBy(SensorReading::getSensorId)
          .reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r1 : r2);
```

### 2. Window 窗口

**文件位置**：`demo/window/WindowDemo.java`

演示三种核心窗口类型：

```java
// 滚动窗口 - 固定大小，不重叠
stream.keyBy(...)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .aggregate(...);

// 滑动窗口 - 固定大小，有重叠
stream.keyBy(...)
      .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
      .aggregate(...);

// 会话窗口 - 动态大小，基于活动间隙
stream.keyBy(...)
      .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
      .process(...);
```

### 3. State 状态管理

**文件位置**：`demo/state/StateDemo.java`

演示 Flink 键控状态的使用：

```java
// ValueState - 单值状态
ValueState<Double> lastTempState = getRuntimeContext()
    .getState(new ValueStateDescriptor<>("last-temp", Double.class));

// ListState - 列表状态
ListState<Double> recentTemps = getRuntimeContext()
    .getListState(new ListStateDescriptor<>("recent-temps", Double.class));

// MapState - 映射状态
MapState<String, Double> locationStats = getRuntimeContext()
    .getMapState(new MapStateDescriptor<>("stats", String.class, Double.class));

// 状态 TTL 配置
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(10))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build();
```

### 4. Checkpoint 检查点

**文件位置**：`demo/checkpoint/CheckpointDemo.java`

演示 Flink 容错机制配置：

```java
// 启用检查点
env.enableCheckpointing(10000);

// 配置检查点
CheckpointConfig config = env.getCheckpointConfig();
config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
config.setCheckpointTimeout(60000);
config.setMinPauseBetweenCheckpoints(500);
config.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 状态后端
env.setStateBackend(new HashMapStateBackend());
// 或使用 RocksDB（适用于大状态）
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
```

### 5. Table API & SQL

**文件位置**：`demo/tableapi/TableApiDemo.java`

演示声明式数据处理：

```java
// 创建 TableEnvironment
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Table API 操作
Table result = sensorTable
    .select($("sensorId"), $("temperature"))
    .where($("temperature").isGreater(30.0))
    .groupBy($("sensorId"))
    .select($("sensorId"), $("temperature").avg().as("avg_temp"));

// Flink SQL
tableEnv.executeSql("""
    SELECT sensor_id, AVG(temperature) as avg_temp
    FROM sensors
    WHERE temperature > 30.0
    GROUP BY sensor_id
""");

// 自定义 UDF
tableEnv.createTemporarySystemFunction("toFahrenheit", ToFahrenheit.class);
```

### 6. CEP 复杂事件处理

**文件位置**：`demo/cep/CepDemo.java`

演示事件模式匹配：

```java
// 定义模式：连续 3 次登录失败
Pattern<UserEvent, ?> pattern = Pattern
    .<UserEvent>begin("login-fail")
    .where(event -> "LOGIN_FAIL".equals(event.getEventType()))
    .times(3)
    .consecutive()
    .within(Time.minutes(1));

// 应用模式
PatternStream<UserEvent> patternStream = CEP.pattern(
    eventStream.keyBy(UserEvent::getUserId),
    pattern
);

// 选择匹配结果
patternStream.select(match -> {
    List<UserEvent> events = match.get("login-fail");
    return "告警: 用户 " + events.get(0).getUserId() + " 连续登录失败";
});
```

### 7. Connectors 连接器

**文件位置**：`demo/connector/ConnectorDemo.java`

演示外部系统连接：

```java
// Kafka Source
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("sensor-data")
    .setGroupId("flink-consumer")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

// JDBC Sink
SinkFunction<Order> jdbcSink = JdbcSink.sink(
    "INSERT INTO orders (order_id, amount) VALUES (?, ?)",
    (ps, order) -> {
        ps.setString(1, order.getOrderId());
        ps.setBigDecimal(2, order.getAmount());
    },
    JdbcExecutionOptions.builder().withBatchSize(1000).build(),
    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://localhost:3306/flink_demo")
        .withDriverName("com.mysql.cj.jdbc.Driver")
        .build()
);
```

## ⚙️ 配置说明

### application.yml

```yaml
server:
  port: 8888

flink:
  execution:
    parallelism: 4
    runtime-mode: streaming
  checkpoint:
    enabled: true
    interval: 60000
    timeout: 600000
    storage-path: file:///tmp/flink-checkpoints
  state-backend:
    type: hashmap
```

## 🔧 核心依赖

| 依赖 | 版本 | 用途 |
|------|------|------|
| flink-streaming-java | 1.18.1 | Flink 流处理核心 |
| flink-clients | 1.18.1 | Flink 客户端 |
| flink-table-api-java-bridge | 1.18.1 | Table API |
| flink-cep | 1.18.1 | 复杂事件处理 |
| flink-connector-kafka | 3.1.0-1.18 | Kafka 连接器 |
| flink-connector-jdbc | 3.1.2-1.18 | JDBC 连接器 |
| flink-statebackend-rocksdb | 1.18.1 | RocksDB 状态后端 |
| spring-boot-starter-web | 3.2.1 | Spring Boot Web |

## 📝 学习建议

1. **入门阶段**：从 `DataStreamBasicDemo` 开始，理解基本的转换操作
2. **进阶阶段**：学习 `WindowDemo` 和 `StateDemo`，掌握窗口和状态管理
3. **高级阶段**：研究 `CheckpointDemo` 和 `CepDemo`，理解容错机制和复杂事件处理
4. **实战阶段**：参考 `ConnectorDemo` 和 `TableApiDemo`，学习与外部系统集成

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 License

MIT License
