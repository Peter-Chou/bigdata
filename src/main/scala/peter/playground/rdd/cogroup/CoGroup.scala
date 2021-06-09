package peter.playground.rdd.cogroup

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CoGroup {

  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val rdd2: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("c", 7)))

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] =
      rdd1.cogroup(rdd2)

    cogroupRDD.collect().foreach(println)

    sc.stop()
  }
}
