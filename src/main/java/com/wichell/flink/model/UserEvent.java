package com.wichell.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

/**
 * 用户行为事件模型
 *
 * 用于演示用户行为分析、CEP 模式匹配等场景
 * 常见场景：点击流分析、用户画像、异常行为检测
 *
 * @author wichell
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 用户 ID
     */
    private String userId;

    /**
     * 事件类型: LOGIN, LOGOUT, CLICK, VIEW, PURCHASE, ADD_CART
     */
    private String eventType;

    /**
     * 页面/资源 ID
     */
    private String pageId;

    /**
     * 事件发生时间戳
     */
    private Long timestamp;

    /**
     * IP 地址 - 用于异常登录检测
     */
    private String ipAddress;

    /**
     * 设备类型: PC, MOBILE, TABLET
     */
    private String deviceType;

    /**
     * 创建登录事件
     */
    public static UserEvent login(String userId, String ip) {
        return UserEvent.builder()
                .userId(userId)
                .eventType("LOGIN")
                .timestamp(System.currentTimeMillis())
                .ipAddress(ip)
                .deviceType("PC")
                .build();
    }

    /**
     * 创建点击事件
     */
    public static UserEvent click(String userId, String pageId) {
        return UserEvent.builder()
                .userId(userId)
                .eventType("CLICK")
                .pageId(pageId)
                .timestamp(System.currentTimeMillis())
                .deviceType("PC")
                .build();
    }
}
