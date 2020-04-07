package com.it.like

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestKafka010 {
  def sparkProcess(args:Array[String])(process:(StreamingContext)=>Unit)={
    var sc:StreamingContext=null
    try{

      sc=StreamingContext.getActiveOrCreate(

        ()=>{
         val sparkConf= new SparkConf().setAppName("hello")
            .setMaster("local[4]")
            .set("spark.streaming.kafka.maxRatePerPartition","10")
              val streamingContext = new StreamingContext(sparkConf,Seconds(5))


          streamingContext.sparkContext.setLogLevel("WARN")
          process(streamingContext)
          streamingContext

        }
      )
      sc.start()
      sc.awaitTermination()

    }catch{
      case e:Exception=>e.printStackTrace()
    }finally {
      if(null!=sc){
        sc.stop(true,true)
      }
      if(offsetUtils.conn!=null){
        offsetUtils.conn.close()
      }
    }
  }
  def process(ssc:StreamingContext): Unit ={
    val topics:Set[String]=Set("test")
    val groupId="bigdata1601"
    val param:Map[String, Object]=Map( "bootstrap.servers" -> "spring:9092,summer:9092,autumn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val locationStrategy: LocationStrategy=LocationStrategies.PreferConsistent
    val offsets: collection.Map[TopicPartition, Long]=offsetUtils.getOffsetRanges(topics,groupId)
   val  consumerStrategy: ConsumerStrategy[String, String]=ConsumerStrategies.Subscribe(topics,param,offsets)
    val kafkaStream : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      locationStrategy,
      consumerStrategy

    )
    var ranges:Array[OffsetRange]=null
    val rsStream: DStream[(String, Int)] = kafkaStream.transform(rdd => {
     ranges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
     // println("offset"+ranges)
      rdd
        //过滤
        .filter(record => null != record.value() && record.value().trim.length > 0)
        //聚合
        .flatMap(record => record.value().trim.split(" "))
        //变成二元组
        .map((_, 1))
        //聚合
        .reduceByKey(_ + _)
    })
    rsStream.foreachRDD((rdd,time)=>{

      if(!rdd.isEmpty()){
        println("=============================")
        val formatTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
        println(formatTime)
        rdd.foreach(println)



        if(null!=ranges){
      offsetUtils.putOffsetRanges(ranges,groupId)
        }
      }


    })

    

  }
  def main(args: Array[String]): Unit = {
    sparkProcess(args)(process)
  }

}
