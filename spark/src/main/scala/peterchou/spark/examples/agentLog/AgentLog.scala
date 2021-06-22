package peterchou.spark.examples.agentLog

// 统计 每一个省份每个广告被点击数量的Top 3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AgentLog {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("agentLog")
    val sc: SparkContext = new SparkContext(conf)

    // timestamp province city user ad
    val fileRDD: RDD[String] = sc.textFile("../data/agent", 2)
    val mapRDD = fileRDD.map(row => {
      val data = row.split(" ")
      val key = (data(1), data(4))
      (key, 1)
    })

    // reduceRDD: ((province, ad), view_num)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    // reduceRDD: (province, (ad, view_num))
    val caseRDD = reduceRDD.map { case ((prov, ad), sum) => (prov, (ad, sum)) }
    val groupRDD = caseRDD.groupByKey()
    val resultRDD = groupRDD.mapValues(iter =>
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
    resultRDD.collect().foreach(println)
    sc.stop()

  }
}
