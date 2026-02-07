package com.wichell.flink.demo.cep;

import com.wichell.flink.model.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Flink CEP (Complex Event Processing) 复杂事件处理演示
 *
 * CEP 用于在事件流中检测复杂的事件模式，常用于：
 * - 异常检测
 * - 欺诈识别
 * - 用户行为分析
 * - 业务流程监控
 *
 * ==================== CEP 核心概念 ====================
 *
 * 1. Pattern（模式）
 *    - 定义要匹配的事件序列
 *    - 支持多种模式组合
 *
 * 2. Pattern API
 *    - begin: 模式的开始
 *    - next: 严格连续（中间不能有其他事件）
 *    - followedBy: 宽松连续（中间可以有其他事件）
 *    - followedByAny: 非确定性宽松连续
 *    - notNext: 不期望的严格连续
 *    - notFollowedBy: 不期望的宽松连续
 *
 * 3. 量词（Quantifier）
 *    - oneOrMore: 一次或多次
 *    - times: 精确次数
 *    - timesOrMore: 至少 N 次
 *    - optional: 可选
 *    - greedy: 贪婪匹配
 *
 * 4. 条件（Condition）
 *    - SimpleCondition: 简单条件，只依赖当前事件
 *    - IterativeCondition: 迭代条件，可以访问之前匹配的事件
 *
 * @author wichell
 */
@Component
public class CepDemo {

    /**
     * 演示简单的 CEP 模式匹配
     *
     * 场景：检测用户连续登录失败
     * 模式：在 1 分钟内，同一用户连续 3 次登录失败
     */
    public void demonstrateSimplePattern(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== CEP 简单模式匹配演示 ==========");

        DataStream<UserEvent> eventStream = createUserEventSource(env);

        // ==================== 1. 定义模式 ====================
        /*
         * 模式：连续 3 次登录失败
         *
         * Pattern.begin("start"): 模式开始，命名为 "start"
         * .where(...): 匹配条件
         * .times(3): 连续匹配 3 次
         * .consecutive(): 严格连续（中间不能有其他事件）
         * .within(...): 时间窗口限制
         */
        Pattern<UserEvent, ?> loginFailPattern = Pattern
                .<UserEvent>begin("login-fail")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "LOGIN_FAIL".equals(event.getEventType());
                    }
                })
                .times(3)  // 连续 3 次
                .consecutive()  // 严格连续
                .within(Time.minutes(1));  // 1 分钟内

        // ==================== 2. 将模式应用到数据流 ====================
        PatternStream<UserEvent> patternStream = CEP.pattern(
                eventStream.keyBy(UserEvent::getUserId),  // 按用户分组
                loginFailPattern
        );

        // ==================== 3. 选择匹配的事件 ====================
        DataStream<String> alertStream = patternStream.select(
                new PatternSelectFunction<UserEvent, String>() {
                    @Override
                    public String select(Map<String, List<UserEvent>> pattern) {
                        // 获取匹配的事件列表
                        List<UserEvent> failEvents = pattern.get("login-fail");

                        UserEvent firstEvent = failEvents.get(0);
                        return String.format(
                                "⚠️ 安全告警: 用户 %s 在 1 分钟内连续 %d 次登录失败，IP: %s",
                                firstEvent.getUserId(),
                                failEvents.size(),
                                firstEvent.getIpAddress()
                        );
                    }
                }
        );

        alertStream.print("登录失败告警");
    }

    /**
     * 演示复杂模式匹配
     *
     * 场景：检测异常交易模式
     * 模式：用户登录后，在 10 分钟内进行大额交易（> 10000）
     */
    public void demonstrateComplexPattern(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== CEP 复杂模式匹配演示 ==========");

        DataStream<UserEvent> eventStream = createUserEventSource(env);

        // ==================== 定义复杂模式 ====================
        /*
         * 模式序列：
         * 1. 用户登录
         * 2. 接着进行大额交易（宽松连续，中间可以有其他操作）
         */
        Pattern<UserEvent, ?> suspiciousPattern = Pattern
                // 开始：用户登录
                .<UserEvent>begin("login")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "LOGIN".equals(event.getEventType());
                    }
                })
                // 宽松连续：大额交易
                .followedBy("large-transaction")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "PURCHASE".equals(event.getEventType());
                        // 实际场景中会检查金额
                    }
                })
                // 10 分钟内
                .within(Time.minutes(10));

        // 应用模式
        PatternStream<UserEvent> patternStream = CEP.pattern(
                eventStream.keyBy(UserEvent::getUserId),
                suspiciousPattern
        );

        // 处理匹配结果
        DataStream<String> alertStream = patternStream.select(
                (Map<String, List<UserEvent>> pattern) -> {
                    UserEvent loginEvent = pattern.get("login").get(0);
                    UserEvent purchaseEvent = pattern.get("large-transaction").get(0);

                    return String.format(
                            "🚨 可疑交易: 用户 %s 登录后 (IP: %s) 立即进行大额交易",
                            loginEvent.getUserId(),
                            loginEvent.getIpAddress()
                    );
                }
        );

        alertStream.print("可疑交易告警");
    }

    /**
     * 演示带超时的模式匹配
     *
     * 场景：订单支付超时检测
     * 模式：订单创建后 15 分钟内未支付
     */
    public void demonstratePatternWithTimeout(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== CEP 超时模式演示 ==========");

        DataStream<UserEvent> eventStream = createUserEventSource(env);

        // 定义侧输出标签用于超时事件
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {};

        // ==================== 定义模式 ====================
        Pattern<UserEvent, ?> orderPattern = Pattern
                // 订单创建
                .<UserEvent>begin("order-created")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "ORDER_CREATED".equals(event.getEventType());
                    }
                })
                // 接着支付完成
                .followedBy("order-paid")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "ORDER_PAID".equals(event.getEventType());
                    }
                })
                // 15 分钟超时（演示用 1 分钟）
                .within(Time.minutes(1));

        // 应用模式
        PatternStream<UserEvent> patternStream = CEP.pattern(
                eventStream.keyBy(UserEvent::getUserId),
                orderPattern
        );

        // ==================== 使用 PatternProcessFunction 处理匹配和超时 ====================
        SingleOutputStreamOperator<String> resultStream = patternStream.process(
                new PatternProcessFunction<UserEvent, String>() {
                    @Override
                    public void processMatch(Map<String, List<UserEvent>> match,
                                             Context ctx,
                                             Collector<String> out) {
                        // 正常匹配：订单已支付
                        UserEvent orderEvent = match.get("order-created").get(0);
                        UserEvent paidEvent = match.get("order-paid").get(0);

                        out.collect(String.format(
                                "✅ 订单支付成功: 用户 %s 的订单已完成支付",
                                orderEvent.getUserId()
                        ));
                    }
                }
        );

        resultStream.print("订单状态");
    }

    /**
     * 演示迭代条件
     *
     * 场景：检测价格持续上涨的股票
     * 模式：连续 3 次价格上涨，且每次涨幅都比上次大
     */
    public void demonstrateIterativeCondition(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== CEP 迭代条件演示 ==========");

        DataStream<UserEvent> eventStream = createUserEventSource(env);

        // ==================== 使用迭代条件 ====================
        /*
         * IterativeCondition 可以访问之前匹配的事件
         * 用于需要比较历史事件的场景
         */
        Pattern<UserEvent, ?> increasingPattern = Pattern
                .<UserEvent>begin("first")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "CLICK".equals(event.getEventType());
                    }
                })
                .followedBy("second")
                .where(new IterativeCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent current, Context<UserEvent> ctx) throws Exception {
                        // 获取之前匹配的事件
                        Iterable<UserEvent> firstEvents = ctx.getEventsForPattern("first");
                        UserEvent firstEvent = firstEvents.iterator().next();

                        // 比较逻辑（示例：检查时间戳是否增加）
                        return current.getTimestamp() > firstEvent.getTimestamp()
                                && "CLICK".equals(current.getEventType());
                    }
                })
                .followedBy("third")
                .where(new IterativeCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent current, Context<UserEvent> ctx) throws Exception {
                        Iterable<UserEvent> secondEvents = ctx.getEventsForPattern("second");
                        UserEvent secondEvent = secondEvents.iterator().next();

                        return current.getTimestamp() > secondEvent.getTimestamp()
                                && "CLICK".equals(current.getEventType());
                    }
                })
                .within(Time.minutes(5));

        PatternStream<UserEvent> patternStream = CEP.pattern(
                eventStream.keyBy(UserEvent::getUserId),
                increasingPattern
        );

        DataStream<String> result = patternStream.select(pattern -> {
            List<UserEvent> first = pattern.get("first");
            List<UserEvent> second = pattern.get("second");
            List<UserEvent> third = pattern.get("third");

            return String.format(
                    "用户 %s 连续点击了 3 次: %s -> %s -> %s",
                    first.get(0).getUserId(),
                    first.get(0).getPageId(),
                    second.get(0).getPageId(),
                    third.get(0).getPageId()
            );
        });

        result.print("连续点击");
    }

    /**
     * 演示量词的使用
     *
     * 场景：检测用户活跃度
     * 模式：用户在 5 分钟内至少点击 5 次
     */
    public void demonstrateQuantifiers(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== CEP 量词演示 ==========");

        DataStream<UserEvent> eventStream = createUserEventSource(env);

        // ==================== 1. times(n) - 精确 N 次 ====================
        Pattern<UserEvent, ?> exactPattern = Pattern
                .<UserEvent>begin("clicks")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "CLICK".equals(event.getEventType());
                    }
                })
                .times(5)  // 精确 5 次
                .within(Time.minutes(5));

        // ==================== 2. oneOrMore() - 一次或多次 ====================
        Pattern<UserEvent, ?> oneOrMorePattern = Pattern
                .<UserEvent>begin("views")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "VIEW".equals(event.getEventType());
                    }
                })
                .oneOrMore()  // 至少 1 次
                .greedy()  // 贪婪匹配，尽可能多匹配
                .within(Time.minutes(5));

        // ==================== 3. times(from, to) - 范围次数 ====================
        Pattern<UserEvent, ?> rangePattern = Pattern
                .<UserEvent>begin("actions")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return event.getEventType() != null;
                    }
                })
                .times(3, 5)  // 3 到 5 次
                .within(Time.minutes(5));

        // ==================== 4. timesOrMore(n) - 至少 N 次 ====================
        Pattern<UserEvent, ?> atLeastPattern = Pattern
                .<UserEvent>begin("purchases")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "PURCHASE".equals(event.getEventType());
                    }
                })
                .timesOrMore(3)  // 至少 3 次
                .within(Time.minutes(10));

        // ==================== 5. optional() - 可选 ====================
        Pattern<UserEvent, ?> optionalPattern = Pattern
                .<UserEvent>begin("login")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "LOGIN".equals(event.getEventType());
                    }
                })
                .next("view")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "VIEW".equals(event.getEventType());
                    }
                })
                .optional()  // 可选的 VIEW 事件
                .next("purchase")
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "PURCHASE".equals(event.getEventType());
                    }
                });

        // 应用第一个模式作为演示
        PatternStream<UserEvent> patternStream = CEP.pattern(
                eventStream.keyBy(UserEvent::getUserId),
                exactPattern
        );

        DataStream<String> result = patternStream.select(pattern -> {
            List<UserEvent> clicks = pattern.get("clicks");
            return String.format(
                    "🎯 活跃用户: %s 在 5 分钟内点击了 %d 次",
                    clicks.get(0).getUserId(),
                    clicks.size()
            );
        });

        result.print("活跃用户");
    }

    /**
     * 演示 AfterMatchSkipStrategy
     *
     * 匹配后跳过策略，决定匹配成功后如何处理后续事件
     */
    public void demonstrateAfterMatchSkipStrategy(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n========== CEP AfterMatchSkipStrategy 演示 ==========");

        DataStream<UserEvent> eventStream = createUserEventSource(env);

        /*
         * AfterMatchSkipStrategy 策略：
         *
         * 1. noSkip() - 不跳过（默认）
         *    所有可能的匹配都会被输出
         *
         * 2. skipToNext() - 跳到下一个
         *    跳过匹配的第一个事件，从第二个开始继续匹配
         *
         * 3. skipPastLastEvent() - 跳过最后一个
         *    跳过匹配的所有事件，从下一个事件开始
         *
         * 4. skipToFirst(patternName) - 跳到第一个指定模式
         *    跳到指定模式的第一个事件
         *
         * 5. skipToLast(patternName) - 跳到最后一个指定模式
         *    跳到指定模式的最后一个事件
         */

        // 使用 skipPastLastEvent 策略
        Pattern<UserEvent, ?> pattern = Pattern
                .<UserEvent>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event) {
                        return "CLICK".equals(event.getEventType());
                    }
                })
                .times(3)
                .within(Time.minutes(5));

        PatternStream<UserEvent> patternStream = CEP.pattern(
                eventStream.keyBy(UserEvent::getUserId),
                pattern
        );

        DataStream<String> result = patternStream.select(match -> {
            List<UserEvent> events = match.get("start");
            return String.format(
                    "匹配到 %d 次点击，使用 skipPastLastEvent 策略",
                    events.size()
            );
        });

        result.print("跳过策略结果");
    }

    /**
     * 创建用户事件数据源
     */
    private DataStream<UserEvent> createUserEventSource(StreamExecutionEnvironment env) {
        return env.addSource(new UserEventSourceFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                );
    }

    /**
     * 模拟用户事件数据源
     */
    private static class UserEventSourceFunction implements SourceFunction<UserEvent> {
        private volatile boolean running = true;
        private final Random random = new Random();

        private final String[] eventTypes = {"LOGIN", "LOGIN_FAIL", "CLICK", "VIEW",
                "PURCHASE", "ADD_CART", "ORDER_CREATED", "ORDER_PAID", "LOGOUT"};
        private final String[] userIds = {"user_1", "user_2", "user_3"};
        private final String[] pageIds = {"home", "product", "cart", "checkout", "profile"};
        private final String[] ips = {"192.168.1.1", "192.168.1.2", "10.0.0.1"};

        @Override
        public void run(SourceContext<UserEvent> ctx) throws Exception {
            while (running) {
                String userId = userIds[random.nextInt(userIds.length)];
                String eventType = eventTypes[random.nextInt(eventTypes.length)];

                UserEvent event = UserEvent.builder()
                        .userId(userId)
                        .eventType(eventType)
                        .pageId(pageIds[random.nextInt(pageIds.length)])
                        .timestamp(System.currentTimeMillis())
                        .ipAddress(ips[random.nextInt(ips.length)])
                        .deviceType(random.nextBoolean() ? "PC" : "MOBILE")
                        .build();

                ctx.collect(event);
                Thread.sleep(500);  // 每 500 毫秒生成一个事件
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 运行所有 CEP 演示
     */
    public void runAllDemos(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink CEP 复杂事件处理演示");
        System.out.println("=".repeat(60));

        // 选择一个演示运行
        demonstrateSimplePattern(env);
        // demonstrateComplexPattern(env);
        // demonstratePatternWithTimeout(env);
        // demonstrateQuantifiers(env);

        env.execute("CEP Demo");
    }

    /**
     * 异步运行所有 CEP 演示，返回 JobClient 用于作业控制
     */
    public org.apache.flink.core.execution.JobClient runAllDemosAsync(StreamExecutionEnvironment env) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("    Flink CEP 复杂事件处理演示");
        System.out.println("=".repeat(60));

        demonstrateSimplePattern(env);

        return env.executeAsync("CEP Demo");
    }
}
