package peterchou.spark.accumulator.longAccumulator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

// 累加器为分布式共享只写变量

object LongAccumulator {
  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("LongAccumulator")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    // 注意：累加器若使用在转换算子时，在获取累加器值之前必须要有行动算子，不然不会累加
    // N个行动算子，则会多加 N-1 遍
    // 一般情况下，累加器放在行动算子中!
    rdd.foreach(num => {
      // 使用累加器
      sumAcc.add(num)
    })

    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }
}
