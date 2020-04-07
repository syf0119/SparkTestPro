package com.it.like2

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IOTOnline {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("hello")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    sc.sparkContext.setLogLevel("WARN")
import sc.implicits._
    import org.apache.spark.sql.functions._
    /**
      * 使用离线和实时两种方式统计:
    1）、信号强度大于10的设备
    2）、各种设备类型的数量
    3）、各种设备类型的平均信号强度
      * @param args
      */
    val dataFrame: DataFrame = sc.readStream.format("socket")
      .option("host", "autumn")
      .option("port", "9999")
      .load()
  val dataSet: Dataset[String] = dataFrame.as[String]
    val rsFrame: DataFrame = dataSet.filter(line => null != line && line.trim.length > 0)
      .select(
        get_json_object($"value", "$.device").as("device"),
        get_json_object($"value", "$.deviceType").as("deviceType"),
        get_json_object($"value", "$.signal").as("signal"),
        get_json_object($"value", "$.time").as("time")

      ).filter($"signal" > 10)
      .groupBy($"deviceType")
      .agg(
        count($"device").as("cnt"),
        avg($"signal").as("avg")
      )


    rsFrame.writeStream.format("console")
      .option("truncate","false")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()



  }

}
