package peter.playground.examples.pageflow

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
    val pageCnt = actionRDD
      .map(action => {
        (action.page_id, 1)
      })
      .reduceByKey(_ + _)
      .collect()

    // 计算分子
    val sessionRDD = actionRDD.groupBy(action => action.session_id)

    val sortRDD =
      sessionRDD.mapValues(iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
        val pageFlowIds = flowIds.zip(flowIds.tail)
        pageFlowIds.map(t => (t, 1))
      })

    // FIXME finish it

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
