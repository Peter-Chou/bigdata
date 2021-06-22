package peterchou.spark.rdd.reduceByKey

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReduceByKey {
  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // reduceByKey 分区内和分区见的合并算法相同
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      x + y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()
  }
}
