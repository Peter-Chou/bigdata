package peter.playground.transformations.aggregateByKey

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
      sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // 分区1: (a, [1, 2])
    // 分区2: (a, [3, 4])
    // (a, 2), (a, 4)
    // (a, 6)
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
