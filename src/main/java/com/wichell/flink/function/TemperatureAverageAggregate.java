package com.wichell.flink.function;

import com.wichell.flink.model.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 温度平均值聚合函数
 *
 * 用于窗口操作中计算传感器温度的平均值
 *
 * 使用增量聚合方式，避免存储所有数据：
 * - 累加器存储温度总和和数据条数
 * - 每来一条数据就更新累加器
 * - 最后计算平均值
 *
 * 适用场景：
 * - 滚动窗口中的平均温度统计
 * - 滑动窗口中的移动平均计算
 * - 会话窗口中的会话平均值
 *
 * 使用示例：
 * <pre>
 * DataStream&lt;Double&gt; avgTempStream = sensorStream
 *     .keyBy(SensorReading::getSensorId)
 *     .window(TumblingEventTimeWindows.of(Time.seconds(10)))
 *     .aggregate(new TemperatureAverageAggregate());
 * </pre>
 *
 * @author wichell
 */
public class TemperatureAverageAggregate
        implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

    /**
     * 创建累加器
     *
     * @return 初始累加器 (温度总和=0.0, 数据条数=0)
     */
    @Override
    public Tuple2<Double, Integer> createAccumulator() {
        return Tuple2.of(0.0, 0);
    }

    /**
     * 累加每条数据
     *
     * @param reading 新到达的传感器数据
     * @param acc 当前累加器
     * @return 更新后的累加器
     */
    @Override
    public Tuple2<Double, Integer> add(SensorReading reading, Tuple2<Double, Integer> acc) {
        return Tuple2.of(acc.f0 + reading.getTemperature(), acc.f1 + 1);
    }

    /**
     * 获取最终结果
     *
     * @param acc 最终累加器
     * @return 平均温度值
     */
    @Override
    public Double getResult(Tuple2<Double, Integer> acc) {
        return acc.f1 > 0 ? acc.f0 / acc.f1 : 0.0;
    }

    /**
     * 合并累加器
     *
     * 用于会话窗口合并或并行聚合场景
     *
     * @param acc1 累加器1
     * @param acc2 累加器2
     * @return 合并后的累加器
     */
    @Override
    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc1, Tuple2<Double, Integer> acc2) {
        return Tuple2.of(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
    }
}
