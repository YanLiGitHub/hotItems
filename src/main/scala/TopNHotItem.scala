import java.sql.Timestamp
import java.util
import java.util.Comparator

import HotItemTest.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @version 1.0
 * @author YanLi
 * @date 2019-08-22 13:49
 */
class TopNHotItem extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
  private var topSize = 0

  def this(topSize: Int) {
    this()
    this.topSize = topSize
  }

  //用于存储商品与点击数的状态，收齐同一窗口的数据，再触发TopN计算
  private var itemState: ListState[ItemViewCount] =null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(itemStateDesc)
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = { //把每条数据添加到状态中
    itemState.add(value)
    //注册windowend + 1的 eventTime
    ctx.timerService.registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems:util.ArrayList[ItemViewCount] = new util.ArrayList()
    val iterator: util.Iterator[ItemViewCount] = itemState.get().iterator()
    while (iterator.hasNext){
      allItems.add(iterator.next())
    }
    //清除状态中的数据，释放空间
    itemState.clear()
    //定义排序规则（降序）
    allItems.sort(new Comparator[ItemViewCount]() {
      override def compare(o1: ItemViewCount, o2: ItemViewCount): Int = (o2.viewCount - o1.viewCount).toInt
    })
    //将需要打印的信息转换成string，方便打印
    val result = new StringBuilder
    result.append("================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    var i = 0
    while (i < allItems.size && i < topSize) {
      val currentItem = allItems.get(i)
      //No1:  商品ID=1111   浏览量=11
      result.append("No")
        .append(i)
        .append(":")
        .append("   商品ID=")
        .append(currentItem.itemId)
        .append("   浏览量=")
        .append(currentItem.viewCount)
        .append("\n")

      i += 1
    }
    result.append("================================\n\n")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString)
  }


}
