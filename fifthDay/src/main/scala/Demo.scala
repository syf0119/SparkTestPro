import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("hello")
    val streamingContext = new  StreamingContext(sparkConf,Seconds(2))
    streamingContext.sparkContext.setLogLevel("ERROR")

    val dataStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("autumn",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    val tranDStream: DStream[(String, Int)] = dataStream.transform(rdd => {
println("执行转换程序")

        rdd.filter(line => line != null && line.trim.split(" ").size != 0)
          .flatMap(_.split(" "))
          .map(_ -> 1)
          .reduceByKey(_ + _)




    })
    tranDStream.foreachRDD(rdd=>{
      println("执行输出程序")
      if(!rdd.isEmpty()){
        rdd.foreach(println)
      }
    })


    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,true)
  }

}
