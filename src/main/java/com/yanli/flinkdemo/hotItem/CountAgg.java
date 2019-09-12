package com.yanli.flinkdemo.hotItem;

import com.yanli.flinkdemo.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author YanLi
 * @version 1.0
 * @date 2019-08-16 15:55
 * count统计聚合函数实现，每出现一条记录+1
 */
public class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator +1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
