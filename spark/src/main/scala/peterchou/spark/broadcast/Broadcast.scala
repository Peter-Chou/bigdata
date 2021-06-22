package peterchou.spark.broadcast

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.broadcast

// 广播变量：分布式共享只读变量
// spark中闭包数据都是以task为单位发送的,可能会导致在一个Executor中有多分数据，造成数据冗余和大量IO
// 广播变量可以把闭包数据保存到Executor的内存中

object Broadcast {
  def main(args: Array[String]) = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("broadcast")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // 封装为广播变量
    val bc: broadcast.Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1
      .map {
        case (w, c) => {
          val l = bc.value.getOrElse(w, 0)
          (w, (c, l))
        }
      }
      .collect()
      .foreach(println)

    sc.stop()
  }
}
