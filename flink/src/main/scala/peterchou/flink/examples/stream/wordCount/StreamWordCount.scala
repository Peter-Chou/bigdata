package peterchou.flink.examples.stream.wordCount

// 引入隐式转换 + 相关的对象
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    // 接受一个socket 文本流
    val inputDataStream: DataStream[String] =
      env.socketTextStream("localhost", 9999)

    // 进行转换操作
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      // flink 每个算子相互独立，可以独立设置每个算子的并行度
      .setParallelism(3)
      .keyBy(0)
      .sum(1)
      .setParallelism(2)

    resultDataStream.print().setParallelism(1)

    // 启动任务执行
    env.execute("stream word count")
  }
}
