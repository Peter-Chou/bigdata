package peter.spark.examples.pageflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PageFlow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("pageFlow")
    val sc: SparkContext = new SparkContext(conf)

    val fileRDD = sc.textFile("data/userAction/user_visit_action.txt")

    val actionRDD = fileRDD.map(line => {
      val data = line.split("_")
      UserVisitAction(
        data(0),
        data(1).toLong,
        data(2),
        data(3).toLong,
        data(4),
        data(5),
        data(6).toLong,
        data(7).toLong,
        data(8),
        data(9),
        data(10),
        data(11),
        data(12).toLong
      )
    })
    actionRDD.cache()

    // 计算分母
    val pageCntMap = actionRDD
      .map(action => {
        (action.page_id, 1L)
      })
      .reduceByKey(_ + _)
      .collect()
      .toMap

    // 计算分子
    val sessionRDD = actionRDD.groupBy(action => action.session_id)

    val sortRDD =
      sessionRDD.mapValues(iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
        val pageFlowIds = flowIds.zip(flowIds.tail)
        pageFlowIds.map(t => (t, 1))
      })
    val dataRDD = sortRDD.flatMap(_._2).reduceByKey(_ + _)

    dataRDD.foreach {
      case ((pageId1, pageId2), jumpCnt) => {
        val total = pageCntMap.getOrElse(pageId1, 0L)
        println(
          s"页面${pageId1} ->页面${pageId2} 的单跳转换率为： " + (jumpCnt.toDouble / total)
        )
      }
    }
    sc.stop()
  }
}

case class UserVisitAction(
    date: String,
    user_id: Long,
    session_id: String,
    page_id: Long,
    action_time: String,
    search_keyword: String,
    click_category_id: Long,
    click_product_id: Long,
    order_category_ids: String,
    oder_product_ids: String,
    pay_category_ids: String,
    pay_product_ids: String,
    city_ids: Long
)
