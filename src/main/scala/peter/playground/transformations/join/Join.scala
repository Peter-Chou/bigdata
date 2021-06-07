package peter.playground.transformations.join

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Join {

  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val rdd2: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("d", 7)))

    // join 两个不同数据源的rdd，相同key的数据连接在一起形成tuple
    // 如果key没有在两个rdd中都出现，则key不会被保留
    // 如果两个rdd中key有多个相同的，会依次匹配，可能会出现迪卡尔乘积, 可能OOM
    // 谨慎使用
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)

    sc.stop()
  }
}
