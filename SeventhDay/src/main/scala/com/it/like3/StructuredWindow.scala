package com.it.like3

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredWindow {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("warn")

    val dataFrame: DataFrame = sparkSession.readStream.format("socket")
      .option("host", "autumn")
      .option("port", 9999)
      .load()

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val dataSet: Dataset[String] = dataFrame.as[String]
    val etlDataFrame: DataFrame = dataSet.filter(line => null != line && line.trim.length > 0)
      .flatMap(line => {
        val Array(timeStr, words) = line.split(",")
        words.split("\\s+").map(Timestamp.valueOf(timeStr) -> _)
      }).toDF("timestamp", "word")

    val result: DataFrame = etlDataFrame.select($"timestamp", $"word")

      .groupBy(
      window($"timestamp","3 seconds","2 seconds"),
        $"word")

      .count()
      .orderBy($"window")
    result.writeStream.outputMode(OutputMode.Complete())
.format("console")
        .option("truncate",false)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
  }


}
