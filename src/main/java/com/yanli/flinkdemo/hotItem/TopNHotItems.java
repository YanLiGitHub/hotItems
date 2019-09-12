package com.yanli.flinkdemo.hotItem;

import com.yanli.flinkdemo.pojo.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author YanLi
 * @version 1.0
 * @date 2019-08-16 16:12
 * 求某个窗口的前N名的热门商品
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount,String> {
    private final int topSize;
    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }
    //用于存储商品与点击数的状态，收齐同一窗口的数据，再触发TopN计算
    private ListState<ItemViewCount> itemState;

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        //把每条数据添加到状态中
        itemState.add(value);
        //注册windowend + 1的 eventTime
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCount> allItems = new ArrayList<>();

         for (ItemViewCount item : itemState.get()){
            allItems.add(item);
        }
        //清除状态中的数据，释放空间
        itemState.clear();
        //定义排序规则（降序）
        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.viewCount - o1.viewCount);
            }
        });
        //将需要打印的信息转换成string，方便打印
        StringBuilder result = new StringBuilder();
        result.append("================================\n");
        result.append("时间：").append(new Timestamp(timestamp-1)).append("\n");
        for (int i=0;i<allItems.size() && i< topSize;i++){
            ItemViewCount currentItem = allItems.get(i);
            //No1:  商品ID=1111   浏览量=11
            result.append("No")
                    .append(i)
                    .append(":")
                    .append("   商品ID=")
                    .append(currentItem.itemId)
                    .append("   浏览量=")
                    .append(currentItem.viewCount)
                    .append("\n");
        }
        result.append("================================\n\n");

        //控制输出频率
        Thread.sleep(1000);

        out.collect(result.toString());

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<>("itemState-state", ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemStateDesc);
    }
}
