package com.it.like2

import org.apache.spark.sql.{DataFrame, SparkSession}

object IOTOffline {
  /**
    * 使用离线和实时两种方式统计:
    1）、信号强度大于10的设备
    2）、各种设备类型的数量
    3）、各种设备类型的平均信号强度
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions","2")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")


    val dataFrame: DataFrame = sparkSession.read.json("SeventhDay/data/device/device.json")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val rsDataframe: DataFrame = dataFrame.filter($"signal" > 10)
      .groupBy($"deviceType")
      .agg(
        count("device").as("cnt"),
        avg($"signal").as("avg")
      ).orderBy($"cnt")
    rsDataframe.printSchema()
    rsDataframe.show()

Thread.sleep(10000000000L)

    sparkSession.stop()

  }

}
