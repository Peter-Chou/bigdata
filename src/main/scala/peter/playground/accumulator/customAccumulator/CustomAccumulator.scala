package peter.playground.accumulator.customAccumulator

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

object CustomAccumulator {
  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala", "hello"))

    val wcAcc = new MyAccumulator()
    // 注册自定义的累加器
    sc.register(wcAcc, "wcAcc")

    rdd.foreach({ word =>
      {
        wcAcc.add(word)
      }
    })

    println(wcAcc.value)

    sc.stop()
  }
}

// 1. 自定义累加器需要继承 AccumulatorV2[IN, OUT], IN为输入的数据类型；OUT为输出的数据类型
// 2. 重写方法
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

  private var wcMap = mutable.Map[String, Long]()

  // 判断是否为初始状态
  override def isZero: Boolean = {
    wcMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new MyAccumulator()
  }

  override def reset(): Unit = {
    wcMap.clear()
  }

  // 获取累加器需要累加的值
  override def add(word: String): Unit = {
    val newCnt = wcMap.getOrElse(word, 0L) + 1
    wcMap.update(word, newCnt)
  }

  // Driver端合并各Executor传回的累加器
  override def merge(
      other: AccumulatorV2[String, mutable.Map[String, Long]]
  ): Unit = {
    val map1 = this.wcMap
    val map2 = other.value

    map2.foreach {
      case (word, count) => {
        val newCnt = map1.getOrElse(word, 0L) + count
        map1.update(word, newCnt)
      }
    }

  }

  // 累加器结果
  override def value: mutable.Map[String, Long] = {
    wcMap
  }

}
