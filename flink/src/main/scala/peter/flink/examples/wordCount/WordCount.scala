package peter.flink.examples.wordCount

import org.apache.flink.api.scala.ExecutionEnvironment

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 引入 flink下的隐式转换
    import org.apache.flink.api.scala._
    val dataset: DataSet[String] = env.readTextFile("../data/wordcount")

    val mapDataset: DataSet[(String, Int)] =
      dataset.flatMap(_.split(" ")).map((_, 1))
    // 以元组中的第一个元素作为key，进行分组
    val groupDataset: GroupedDataSet[(String, Int)] = mapDataset.groupBy(0)
    // 对所有数据的第二个元素进行求和
    val resultDataset: AggregateDataSet[(String, Int)] = groupDataset.sum(1)

    resultDataset.print()

  }
}
