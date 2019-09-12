package com.yanli.flinkdemo.hotItem;

import com.yanli.flinkdemo.pojo.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

/**
 * @author YanLi
 * @version 1.0
 * @date 2019-08-16 15:58
 * 用于输出窗口的结果
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple   //窗口主键，即itemId
            , TimeWindow window     //窗口
            , java.lang.Iterable<Long> input    //聚合函数的结果，即count值
            , Collector<ItemViewCount> out      //输出类型为ItemViewCount
    ) throws Exception {
        Long itemId = ((Tuple1<Long>) tuple).f0;
        Long count = input.iterator().next();
        out.collect(ItemViewCount.getInstance(itemId,window.getEnd(),count));
    }
}
