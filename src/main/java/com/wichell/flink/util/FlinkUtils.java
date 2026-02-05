package com.wichell.flink.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Flink 工具类
 *
 * 提供常用的工具方法：
 * - JSON 序列化/反序列化
 * - 时间格式化
 * - 其他辅助方法
 *
 * @author wichell
 */
@Slf4j
public class FlinkUtils {

    private static final ObjectMapper OBJECT_MAPPER;
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    static {
        OBJECT_MAPPER = new ObjectMapper();
        // 支持 Java 8 时间类型
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        // 格式化输出
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        // 禁用时间戳格式
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * 对象转 JSON 字符串
     *
     * @param obj 对象
     * @return JSON 字符串
     */
    public static String toJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("JSON 序列化失败", e);
            return "{}";
        }
    }

    /**
     * JSON 字符串转对象
     *
     * @param json JSON 字符串
     * @param clazz 目标类型
     * @return 对象
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            log.error("JSON 反序列化失败", e);
            return null;
        }
    }

    /**
     * 时间戳转 LocalDateTime
     *
     * @param timestamp 毫秒时间戳
     * @return LocalDateTime
     */
    public static LocalDateTime timestampToLocalDateTime(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
        );
    }

    /**
     * 格式化时间戳为日期时间字符串
     *
     * @param timestamp 毫秒时间戳
     * @return 格式化的日期时间字符串 (yyyy-MM-dd HH:mm:ss)
     */
    public static String formatTimestamp(long timestamp) {
        return timestampToLocalDateTime(timestamp).format(DATE_TIME_FORMATTER);
    }

    /**
     * 格式化时间戳为时间字符串
     *
     * @param timestamp 毫秒时间戳
     * @return 格式化的时间字符串 (HH:mm:ss.SSS)
     */
    public static String formatTime(long timestamp) {
        return timestampToLocalDateTime(timestamp).format(TIME_FORMATTER);
    }

    /**
     * 获取当前时间戳
     *
     * @return 当前毫秒时间戳
     */
    public static long currentTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * 计算两个时间戳之间的秒数差
     *
     * @param start 开始时间戳
     * @param end 结束时间戳
     * @return 秒数差
     */
    public static long secondsBetween(long start, long end) {
        return (end - start) / 1000;
    }

    /**
     * 打印分隔线
     *
     * @param title 标题
     */
    public static void printSeparator(String title) {
        System.out.println();
        System.out.println("=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }

    /**
     * 打印子标题
     *
     * @param subtitle 子标题
     */
    public static void printSubtitle(String subtitle) {
        System.out.println();
        System.out.println("-".repeat(40));
        System.out.println("  " + subtitle);
        System.out.println("-".repeat(40));
    }

    /**
     * 安全休眠
     *
     * @param millis 毫秒数
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
