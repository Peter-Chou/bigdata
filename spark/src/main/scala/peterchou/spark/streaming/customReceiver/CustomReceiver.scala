package peterchou.spark.streaming.customReceiver

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

import java.util.Random
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object CustomReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val msgDS: ReceiverInputDStream[String] =
      ssc.receiverStream(new MyReceiver())

    msgDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var flag = true

  override def onStart(): Unit = {
    new Thread(() => {
      while (flag) {
        val message = "采集的数据为：" + new Random().nextInt(10).toString()
        store(message)
        Thread.sleep(500)

      }
    }).start()
  }

  override def onStop(): Unit = {
    flag = false
  }

}
