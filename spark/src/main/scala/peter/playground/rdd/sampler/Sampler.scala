package peter.spark.rdd.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Sampler {
  def main(args: Array[String]): Unit = {}
  val conf: SparkConf =
    new SparkConf().setMaster("local[*]").setAppName("filter")
  val sc: SparkContext = new SparkContext(conf)
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  rdd.sample(false, 0.4, 1024).collect().foreach(println)

  sc.stop()
}
