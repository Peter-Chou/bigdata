package peter.playground.transformations.leftJoin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object LeftJoin {

  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val rdd2: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 4), ("b", 5)))

    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] =
      rdd1.leftOuterJoin(rdd2)

    leftJoinRDD.collect().foreach(println)

    sc.stop()
  }
}
