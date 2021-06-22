package peter.spark.streaming.streamingWordCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // StreamingContext 需要两个参数
    // 第一个参数：环境配置
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    // 第二个参数：批量处理的周期（采集周期）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 获取端口数据
    val lines: ReceiverInputDStream[String] =
      ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordsToCount: DStream[(String, Int)] =
      words.map((_, 1)).reduceByKey(_ + _)

    wordsToCount.print()

    // 由于SparkSteaming采集器是长期执行的任务,不能直接关闭
    // main()执行完毕，应用程序会自动结束，所以不能让main()执行完毕
    // 1. 启动采集器
    ssc.start()
    // 2. 等待采集器的关闭
    ssc.awaitTermination()
  }

}
