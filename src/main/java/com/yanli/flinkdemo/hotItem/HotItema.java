package com.yanli.flinkdemo.hotItem;

import com.yanli.flinkdemo.pojo.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 * @author YanLi
 * @version 1.0
 * @date 2019-08-16 15:38
 */
public class HotItema {
    public static void main(String[] args) throws Exception {
        //创建excution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置全局并发数
        env.setParallelism(1);
        //获取resource目录下的CSV文件
        URL url = HotItema.class.getClassLoader().getResource("UserBehavior.csv");

        Path filePath = Path.fromLocalFile(new File(url.toURI()));
        //抽取UserBehavior的TypeInfomation
        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo) TypeExtractor.createTypeInfo(UserBehavior.class);
        //指定抽取出的字段顺序，反射会造成乱序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        //创建PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldOrder);

        DataStreamSource<UserBehavior> datasource = env.createInput(csvInputFormat, pojoTypeInfo);
        //设置系统处理时间为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //抽取时间生成waterMark
        datasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                //时间戳秒变为毫秒
                return userBehavior.timestamp * 1000;
            }

        })
                //过滤出pv操作
                .filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.behavior.equals("pv");
            }
        }).keyBy("itemId")
                .timeWindow(Time.minutes(60),Time.seconds(5))
                .aggregate(new CountAgg(),new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new TopNHotItems(3))
                .print();

        env.execute("Hot Items Job");



    }
}
