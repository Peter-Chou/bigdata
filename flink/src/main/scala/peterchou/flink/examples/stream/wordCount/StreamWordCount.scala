package peterchou.flink.examples.stream.wordCount

// 引入隐式转换 + 相关的对象
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    // 接受一个socket 文本流
    val inputDataStream: DataStream[String] =
      env.socketTextStream("localhost", 9999)

    // 进行转换操作
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    // 启动任务执行
    env.execute("stream word count")
  }
}
