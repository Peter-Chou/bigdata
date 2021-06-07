package peter.playground.transformations.sortby

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SortBy {
  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("sortby")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(3, 1, 2, 7, 4, 5, 6, 8, 9, 10), 2)
    val sortRdd: RDD[Int] = rdd.sortBy(num => num)
    sortRdd.collect().foreach(println)
  }
}
