package peterchou.spark.rdd.aggregateByKey

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AggregateByKey {
  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc: SparkContext = new SparkContext(conf)

    // 目标： 分区内求最大，分区间求和
    val rdd: RDD[(String, Int)] =
      sc.makeRDD(
        List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)),
        2
      )

    // 分区1: (a, [1, 2],), (b, 3)
    // 分区2: (b, [4, 5]), (a, 6)

    // 分区1: (a, 2), (b, 3)
    // 分区2: (b, 5), (a, 6)

    // (a, 8), (b, 8)
    // aggregateByKey 有两个参数列表:
    // 第一个参数列表：
    //    需要传一个参数， 表示为初始值（主要用于碰见第一个key时，和value进行分区内计算）
    // 第二个参数列表：
    //    第一个参数，表示分区内计算规则
    //    第二个参数，表示分区间计算规则
    val aggRDD: RDD[(String, Int)] =
      rdd.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => x + y)
    aggRDD.collect().foreach(println)

    sc.stop()
  }
}
