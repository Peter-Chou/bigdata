package peterchou.spark.sql.basic

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object SqlBasic {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("DataFrame")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 使用DSL时，需要引入转换规则（例如$）
    // org.apache.spark.sql.SparkSession.implicits._
    import spark.implicits._

    // ********** DataFrame *********************

    // DataFrame 即 Dataset[Row]
    val df: DataFrame = spark.read.json("../data/user.json")

    // query by SQL
    // df.createOrReplaceTempView("user")
    // spark.sql("select avg(age) from user").show

    // query by DSL
    // df.select($"age" + 1).show
    df.select('age + 1).show

    // DataFrame => Dataset

    val rdd: RDD[(Int, String, Int)] =
      spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val userDF: DataFrame = rdd.toDF("id", "name", "age")
    val ds: Dataset[User] = userDF.as[User]

    // RDD => Dataset
    val userDS: Dataset[User] = rdd
      .map {
        case (id, name, age) => {
          User(id, name, age)
        }
      }
      .toDS()

    userDS.show

    spark.close()
  }
}

case class User(id: Int, name: String, age: Int)
