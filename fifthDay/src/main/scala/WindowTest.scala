import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("hello")
    val streamingContext = new StreamingContext(sparkConf,Seconds(2))
    streamingContext.sparkContext.setLogLevel("WARN")
    val receiveData: ReceiverInputDStream[String] = streamingContext.socketTextStream("autumn",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    receiveData.window(Seconds(4),Seconds(4)).transform(rdd=>{
      println(new Date())
      rdd.filter(_!=null)
        .flatMap(_.split(" "))
        .map(_->1)
        .reduceByKey(_+_)
        .sortBy(_._2,false)

    }).foreachRDD(rdd=>{
      if(!rdd.isEmpty()){


        println(rdd.first())
      }
    })


    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,true)
  }
}
