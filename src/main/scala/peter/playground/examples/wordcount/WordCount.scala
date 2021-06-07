package peter.playground.examples.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/wordcount")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordsTuple: RDD[(String, Int)] = words.map((_, 1))
    val wordsCount: RDD[(String, Int)] = wordsTuple.reduceByKey(_ + _)
    wordsCount.collect().foreach(println)
    sc.stop()
  }
}
