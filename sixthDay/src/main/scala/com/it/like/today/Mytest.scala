package com.it.like.today

import kafka.serializer.StringDecoder
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object Mytest {

  def sparkProcess(args:Array[String])(process:StreamingContext=>Unit)={
    var ssc:StreamingContext=null
    try{
      val checkpoint="data2/20191202"
      ssc=StreamingContext.getActiveOrCreate(
        checkpoint,
        ()=>{
          val sparkConf= new SparkConf().setMaster("local[4]")
            .setAppName("hello")
            .set("spark.streaming.kafka.maxRatePerPartition","10")
          val context = new StreamingContext(sparkConf,Seconds(5))
          context.sparkContext.setLogLevel("WARN")
          context.checkpoint(checkpoint)

          process(context)
          context
        }
      )

      ssc.sparkContext.setLogLevel("warn")


      ssc.start()
      ssc.awaitTermination()

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if(null!=ssc) ssc.stop(true,true)
    }

  }
  def process(ssc:StreamingContext): Unit ={
    val kafkaParams: Map[String, String]=Map("bootstrap.servers"->"spring:9092,summer:9092,autumn:9092",
      "auto.offset.reset"->"largest")
    val topics: Set[String]=Set("test")
    val kafkaData: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics
    )
    val tranData: DStream[(String, Double)] = kafkaData.transform(
      rdd => {
        rdd.map(_._2).filter(ele => ele != null && ele.trim.split(",").length==3)
          .map(ele => ele.split(",")(1) -> ele.split(",")(2).toDouble)
          .reduceByKey(_ + _)
      }
    )
    val updateStream: DStream[(String, Double)] = tranData.updateStateByKey((cur: Seq[Double], pre: Option[Double]) => {
      val curSum: Double = cur.sum
      val preValue: Double = pre.getOrElse(0)
      Some(curSum + preValue)
    })

    updateStream.foreachRDD((rdd,time)=>{
      val dataTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
      println("==================================")
      println(dataTime)
      rdd.foreach(println(_))
    })


  }
  def main(args: Array[String]): Unit = {
    sparkProcess(args)(process)
  }


}
