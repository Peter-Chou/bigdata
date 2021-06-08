package peter.playground.transformations.persist

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Persist {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("ckpt")

    val lines = sc.makeRDD(List("Hello scala", "Hello Spark"))
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordsTuple: RDD[(String, Int)] = words.map((_, 1))

    // cache: 将数据临时存储在内存中，以便之后数据重用, 会在血缘关系中添加新的依赖。
    // persist: 将数据临时存储在磁盘/内存中进行数据重用,性能较低，如果作业执行完毕,临时保存的数据会丢失，会在血缘关系中添加新的依赖。
    // checkpoint: 将数据长久的保存在磁盘,一般情况下会独立执行作业,为提高效率，一般和cache配合使用
    wordsTuple.cache()
    wordsTuple.checkpoint()
    val wordsCount: RDD[(String, Int)] = wordsTuple.reduceByKey(_ + _)
    wordsCount.collect().foreach(println)
    sc.stop()
  }
}
