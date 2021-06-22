package peter.spark.examples.topk

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import spire.syntax.group

import scala.collection.mutable

// 按照每个品类的点击、下单、支付的量来统计热门品类
// 综合排名 = 点击数 × 20% + 下单数 × 30% + 支付数 × 50%
// 先按照点击数排名，考前的就排名高，如果点击数相同，再比较下单数，下单数相同再比较支付数。

object Topk {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("top10")
    val sc: SparkContext = new SparkContext(conf)

    val acc = new TopkAccumulator
    sc.register(acc, "topkAcc")

    val fileRDD = sc.textFile("../data/userAction/user_visit_action.txt")
    fileRDD.cache()

    val categories = topkCategory(fileRDD, acc)
    println("categories: \n")
    categories.foreach(println)
    val top10CatNames = categories.map(_.cid)

    // Top10热门类别中的Top10 Session统计
    val filterRDD = fileRDD.filter(line => {
      val data = line.split("_")
      if (data(6) != "-1") {
        top10CatNames.contains(data(6))
      } else {
        false
      }
    })
    val reduceRDD = filterRDD
      .map(line => {
        val data = line.split("_")
        ((data(6), data(2)), 1)
      })
      .reduceByKey(_ + _)

    val groupedRDD = reduceRDD
      .map(data => {
        (data._1._1, (data._1._2, data._2))
      })
      .groupByKey()

    val top10SessionRDD = groupedRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })
    val top10sessions = top10SessionRDD.collect()
    println("top 10 sessions of top 10 categories: \n")
    top10SessionRDD.foreach(println)

    sc.stop()

  }
  def topkCategory(
      rdd: RDD[String],
      acc: TopkAccumulator
  ): List[CategoryStats] = {

    // top 10 热门品类
    rdd.foreach(line => {
      val data = line.split("_")
      if (data(6) != "-1") {
        // 点击事件
        // List((data(6), (1, 0, 0)))
        acc.add((data(6), "click"))
      } else if (data(8) != "null") {
        // 下单事件
        val cid = data(8).split(",")
        // cid.map(id => (id, (0, 1, 0)))
        cid.foreach(id => acc.add(id, "order"))
      } else if (data(10) != "null") {
        // 支付事件
        val cid = data(10).split(",")
        // cid.map(id => (id, (0, 0, 1)))
        cid.foreach(id => acc.add(id, "pay"))
      }
    })

    val categories = acc.value.map(_._2)

    val results = categories.toList.sortWith((left, right) => {
      if (left.clickCnt > right.clickCnt) {
        true
      } else if (left.clickCnt == right.clickCnt) {
        if (left.orderCnt > right.orderCnt) {
          true
        } else if (left.orderCnt == right.orderCnt) {
          left.payCnt > right.payCnt
        } else {
          false
        }
      } else {
        false
      }
    })
    results.take(10)

  }
}

case class CategoryStats(
    cid: String,
    var clickCnt: Int,
    var orderCnt: Int,
    var payCnt: Int
)

// 1. 继承自AccumulatorV2
//    IN: (品类ID，行为类型)
//   OUT: (muable.Map[String, CategoryStats])
class TopkAccumulator
    extends AccumulatorV2[
      (String, String),
      mutable.Map[String, CategoryStats]
    ] {
  private var csMap = mutable.Map[String, CategoryStats]()

  override def isZero: Boolean = {
    csMap.isEmpty
  }

  override def copy()
      : AccumulatorV2[(String, String), mutable.Map[String, CategoryStats]] = {
    new TopkAccumulator()
  }

  override def reset(): Unit = { csMap.clear() }

  override def add(v: (String, String)): Unit = {
    val categoryName = v._1
    val eventType = v._2
    val category =
      csMap.getOrElse(categoryName, CategoryStats(categoryName, 0, 0, 0))
    if (eventType == "click") {
      category.clickCnt += 1
    } else if (eventType == "order") {
      category.orderCnt += 1
    } else if (eventType == "pay") {
      category.payCnt += 1
    }
    csMap.update(categoryName, category)
  }

  override def merge(
      other: AccumulatorV2[(String, String), mutable.Map[String, CategoryStats]]
  ): Unit = {
    val map1 = this.csMap
    val map2 = other.value
    map2.foreach {
      case (cName, hc) => {
        val category = map1.getOrElse(cName, CategoryStats(cName, 0, 0, 0))
        category.clickCnt += hc.clickCnt
        category.orderCnt += hc.orderCnt
        category.payCnt += hc.payCnt
        map1.update(cName, category)
      }
    }
  }

  override def value: mutable.Map[String, CategoryStats] = csMap

}
