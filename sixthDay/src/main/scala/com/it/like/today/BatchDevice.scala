package com.it.like.today

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
	1.信号强度大于10的设备
	2.各种设备类型的数量
	3.各种设备类型的平均信号强度

  */
object BatchDevice {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[4]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val dataFrame: DataFrame = sparkSession.read.format("json").load("F://device.json")
    dataFrame.printSchema()
    dataFrame.show()
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
dataFrame.select($"device",$"signal")
      .filter($"signal">10)
      .show()
dataFrame.select($"deviceType")
      .groupBy(
        $"deviceType"
      )
      .agg(
        count($"deviceType").as("cnt")
      ).show()

    dataFrame.select($"deviceType",$"signal")
      .groupBy($"deviceType")
      .agg(
        avg($"signal")
      )
      .show()

    sparkSession.stop()
  }


}
