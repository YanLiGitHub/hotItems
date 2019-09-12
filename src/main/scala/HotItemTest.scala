

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @version 1.0
 * @author YanLi
 * @date 2019-08-19 15:51
 */
object HotItemTest {
  def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val datastream: DataStream[String] = env.readTextFile("/Users/tal/IdeaProjects/hotItems/src/main/resources/UserBehavior.csv")


        datastream.map(_.split(","))
            .map(x => UserBehavior(x.repr(0).toLong,x.repr(1).toLong,x.repr(2).toInt,x.repr(3),x.repr(4).toLong))
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior] {
        override def extractAscendingTimestamp(element: UserBehavior): Long = {
          element.timestamp * 1000
        }
      })
            .filter(_.behavior == "pv")
            .keyBy("itemId")
            .timeWindow(Time.minutes(60),Time.seconds(5))
            .aggregate(new AggregateFunction[UserBehavior,Long,Long]{
                override def createAccumulator(): Long = 0L

              override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

              override def getResult(accumulator: Long): Long = accumulator

              override def merge(a: Long, b: Long): Long = a + b
            },new WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
              override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
                val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
                val count: Long = input.iterator.next()
                out.collect(ItemViewCount(itemId,window.getEnd,count))
              }
            }).keyBy("windowEnd")
            .process(new TopNHotItem(3))
            .print()


    env.execute("hotItemTest")
  }


  case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long){
    def this(){
      this(0,0,0,null,0)
    }
  }

  case class ItemViewCount(itemId:Long,windowEnd:Long,viewCount:Long)


}
