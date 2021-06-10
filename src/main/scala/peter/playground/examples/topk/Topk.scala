package peter.playground.examples.topk

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import spire.syntax.group

// 按照每个品类的点击、下单、支付的量来统计热门品类
// 综合排名 = 点击数 × 20% + 下单数 × 30% + 支付数 × 50%
// 先按照点击数排名，考前的就排名高，如果点击数相同，再比较下单数，下单数相同再比较支付数。

object Topk {
  def main(args: Array[String]): Unit = {
    // top 10 热门品类
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("top10")
    val sc: SparkContext = new SparkContext(conf)

    val fileRDD = sc.textFile("data/userAction/user_visit_action.txt")

    val clickRDD = fileRDD.filter(line => {
      val data = line.split("_")
      data(6) != "-1"
    })
    val clickCountRDD = clickRDD
      .map(line => {
        val data = line.split("_")
        (data(6), 1)
      })
      .reduceByKey(_ + _)

    val orderRDD = fileRDD.filter(line => {
      val data = line.split("_")
      data(8) != "null"
    })
    val orderCountRDD = orderRDD
      .flatMap(line => {
        val data = line.split("_")
        val idsString: String = data(8)
        val cids = idsString.split(",")
        cids.map(id => (id, 1))
      })
      .reduceByKey(_ + _)

    val payRDD = fileRDD.filter(line => {
      val data = line.split("_")
      data(10) != "null"
    })
    val payCountRDD = payRDD
      .flatMap(line => {
        val data = line.split("_")
        val idsString: String = data(10)
        val cids = idsString.split(",")
        cids.map(id => (id, 1))
      })
      .reduceByKey(_ + _)

    val cogroupRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        if (clickIter.iterator.hasNext) {
          clickCnt = clickIter.iterator.next()
        }
        var orderCnt = 0
        if (orderIter.iterator.hasNext) {
          orderCnt = orderIter.iterator.next()
        }
        var payCnt = 0
        if (payIter.iterator.hasNext) {
          payCnt = payIter.iterator.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)

    sc.stop()

  }
}
