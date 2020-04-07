package com.it.like.today


import kafka.serializer.StringDecoder
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Frank on 2019/12/1.
  * 基于借贷模式来实现模板
  */
object Test2 {

  //贷出函数：主要用于申请资源和回收资源
  def sparkProcess(args: Array[String])(process:StreamingContext => Unit ): Unit ={

    //构建StreamingContext对象
    var ssc:StreamingContext = null
    try{
      /**
        * 创建一个StreamingContext实例
        *  def getActiveOrCreate(
      checkpointPath: String,
      creatingFunc: () => StreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    ): StreamingContext = {
        */
      val checkPoint = "datas/spark/stream/chk/order02"
      ssc = StreamingContext.getActiveOrCreate(
        checkPoint,
        () =>{
          //构建配置对象
          val conf = new SparkConf()
            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
            .setMaster("local[4]")
            .set("spark.streaming.kafka.maxRatePerPartition","10")
          //构建ssc对象
          val context = new StreamingContext(conf,Seconds(5))
          //设置日志级别
          context.sparkContext.setLogLevel("WARN")
          //设置checkpoint
          context.checkpoint(checkPoint)
          //调用处理的方法来执行
          process(context)
          context
        }
      )
      //设置日志级别
      ssc.sparkContext.setLogLevel("WARN")
      //启动程序
      ssc.start()
      //持久运行
      ssc.awaitTermination()
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      //关闭资源
      if (null != ssc) ssc.stop(true,true)
    }
  }

  //主要用于处理数据的逻辑代码
  def processData(ssc:StreamingContext): Unit ={
    //todo:1-读取数据
    val  kafkaParams: Map[String, String] = Map("bootstrap.servers"->"spring:9092,summer:9092,autumn:9092"
      ,"auto.offset.reset"->"largest")
    val  topics: Set[String] = Set("test")
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaParams,
      topics
    )
    //todo:2-转换数据
    //直接对当前批次的数据进行分析处理
    val dataStream: DStream[(Int, Double)] = kafkaStream.transform(rdd => {
      rdd
        //过滤，把value取出来，并且过滤为空的
        .filter(tuple => tuple._2 != null && tuple._2.trim.split(",").length == 3 )
        //进行转换处理
        .mapPartitions(part => {
        part.map(tuple => {
          //对value进行分割，得到三个字段：订单id，省份id，金额
          val Array(orderId,province,orderAmt) = tuple._2.trim.split(",")
          (province.toInt,orderAmt.toDouble)
        })
      })
        //拿到了每个省份的每一条订单的金额，对省份进行聚合
        .reduceByKey(_+_)
    })

    //进行有状态的 叠加统计
    val rsStream: DStream[(Int, Double)] = dataStream.updateStateByKey(
      (current:Seq[Double],previous:Option[Double]) => {
        //先聚合当前的所有值
        val currValue = current.sum
        //获取之前的值
        val preValue = previous.getOrElse(0.0)
        //计算最新的 值
        val lastValue = currValue + preValue
        Some(lastValue)
      }
    )

    //todo:3-输出数据
    rsStream
      //过滤
      .filter(tuple => tuple._1 > 10)
      //打印输出
      .foreachRDD((rdd,time) => {
      //标准格式：yyyy-MM-dd HH:mm:ss
      val dateTime = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
      println("=========================================")
      println(dateTime)
      println("=========================================")
      rdd.foreachPartition(part => {
        part
          //过滤省份id大于10的
          //          .filter(tuple => tuple._1 > 10)
          .foreach(println)
      })
    })
  }



  /**
    * 整个程序的入口
    * @param args
    */
  def main(args: Array[String]) {
    //调用贷出函数
    sparkProcess(args)(processData)
  }

}
