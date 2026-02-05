package com.wichell.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单数据模型
 *
 * 用于演示电商场景下的订单流处理
 * 包含订单统计、金额聚合等操作
 *
 * @author wichell
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 订单 ID
     */
    private String orderId;

    /**
     * 用户 ID - 可用于用户维度的聚合分析
     */
    private String userId;

    /**
     * 商品 ID
     */
    private String productId;

    /**
     * 商品类别 - 用于分类统计
     */
    private String category;

    /**
     * 订单金额
     */
    private BigDecimal amount;

    /**
     * 订单状态: CREATED, PAID, SHIPPED, COMPLETED, CANCELLED
     */
    private String status;

    /**
     * 订单创建时间戳（事件时间）
     */
    private Long createTime;

    /**
     * 支付时间戳
     */
    private Long payTime;

    /**
     * 创建测试订单
     */
    public static Order create(String orderId, String userId, BigDecimal amount) {
        return Order.builder()
                .orderId(orderId)
                .userId(userId)
                .productId("P" + System.currentTimeMillis())
                .category("default")
                .amount(amount)
                .status("CREATED")
                .createTime(System.currentTimeMillis())
                .build();
    }
}
