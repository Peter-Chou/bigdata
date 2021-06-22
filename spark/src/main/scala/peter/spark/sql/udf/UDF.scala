package peter.spark.sql.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

// UDF => User Defined Function
// select 语句中的自定义函数
object UDF {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("DataFrame")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    spark.udf.register(
      "prefixName",
      (name: String) => {
        "Name: " + name
      }
    )

    val df: DataFrame = spark.read.json("data/user.json")

    df.createOrReplaceTempView("user")

    spark.sql("select age, prefixName(username) from user").show
    // result:
    // +---+--------------------+
    // |age|prefixName(username)|
    // +---+--------------------+
    // | 20|      Name: zhangsan|
    // | 30|          Name: lisi|
    // | 40|        Name: wangwu|
    // +---+--------------------+

    spark.close()
  }
}
