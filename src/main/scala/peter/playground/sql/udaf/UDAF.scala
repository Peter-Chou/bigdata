package peter.playground.sql.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions

// UserDefinedAggregateFunction
// 用户自定义聚合函数

object UDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("DataFrame")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.sql("select ageAvg(age) from user").show

    spark.close()
  }
}

// 自定义聚合函数
// 1. 继承 org.apache.spark.sql.expressions.Aggregator, 定义范型
//   IN : 输入的数据类型 Long
//   BUF: 缓冲区的数据类型 Buff
//   OUT: 输出的数据类型 Long

case class Buff(var total: Long, var count: Long)

class MyAvgUDAF extends Aggregator[Long, Buff, Long] {

  // 缓冲区的初始化
  override def zero: Buff = {
    Buff(0L, 0L)
  }

  // 根据输入的数据来更新缓冲区的数据
  override def reduce(buff: Buff, in: Long): Buff = {
    buff.total = buff.total + in
    buff.count = buff.count + 1
    buff
  }

  // 合并缓冲区
  override def merge(buff1: Buff, buff2: Buff): Buff = {
    buff1.total = buff1.total + buff2.total
    buff1.count = buff1.count + buff2.count
    buff1
  }

  // 计算结果
  override def finish(buff: Buff): Long = {
    buff.total / buff.count
  }

  // 缓冲区的编码操作
  // 自定义类型使用 Encoders.project
  // scala自带的类型使用 Encoders.scala*
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  // 缓冲区的解码操作
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong

}
