import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWithStatus {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("hello")
    val streamingContext = new StreamingContext(sparkConf,Seconds(2))
    streamingContext.checkpoint("data/checkpoint")
    streamingContext.sparkContext.setLogLevel("ERROR")
    val dataStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("autumn",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    val transfStream: DStream[(String, Int)] = dataStream.transform(rdd => {
      rdd.filter(line => line != null && line.trim.split(" ").size > 0)
        .flatMap(_.split(" "))
        .map(_ -> 1)
        .reduceByKey(_ + _)
    })
    val updateStream: DStream[(String, Int)] = transfStream.updateStateByKey((cur: Seq[Int], pre: Option[Int]) => {


      val curSum: Int = cur.sum


      val preValue: Int = pre.getOrElse(0)
      Some(curSum + preValue)
    }
    )

    updateStream.foreachRDD(rdd=>{
  rdd.foreach(println)
      println("++++++++++++++")
})







    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,true)
  }

}
