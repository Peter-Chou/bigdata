package peter.playground.transformations.partitioner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark

object Partitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: spark.rdd.RDD[(String, Int)] =
      sc.makeRDD(List(("nba", 1), ("wnba", 2), ("nba", 3), ("cba", 4)), 3)
    val parRDD: spark.rdd.RDD[(String, Int)] =
      rdd.partitionBy(new MyPartitioner)

    parRDD.saveAsTextFile("output/partitioner")

    sc.stop()
  }
}

class MyPartitioner extends spark.Partitioner {

  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    key match {
      case "nba"  => 0
      case "wnba" => 1
      case _      => 2
    }
  }
}
