package com.it.like

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ReadKafka {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]")
      .appName("hello")
      .config("spark.sql.shuffle.partitions","2")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val sourceData: DataFrame = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "spring:9092,summer:9092,autumn:9092")
      .option("subscribe", "test")
      .load()
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //提取出三个字段:
    // {"orderId":"ec844809-34cf-47f3-ae42-ea2b7dfc9cfe","provinceId":33,"orderPrice":49.5}
//    2-过滤：省份id小于等于10，并且订单价格大于等于50
    //    3-写入Kafka
    val sourceDataset: Dataset[String] = sourceData.selectExpr("CAST(value AS STRING)").as[String]
    val rsDataFrame : DataFrame = sourceDataset.filter(line => null != line && line.trim.length > 0)
      .select(get_json_object($"value", "$.orderId").as("orderId"),
        get_json_object($"value", "$.provinceId")as("provinceId"),
        get_json_object($"value", "$.orderPrice").as("orderPrice")
      ).filter($"provinceId".leq(10).and($"orderPrice".geq(50)))

    val sinkDataFrame: DataFrame = rsDataFrame.select(
      $"orderId".as("key"),
      to_json(struct($"provinceId", $"orderPrice")).as("value")
    )




    sinkDataFrame.writeStream.outputMode(OutputMode.Append)
          .format("kafka")
        .option("kafka.bootstrap.servers", "spring:9092,summer:9092,autumn:9092")
        .option("topic","test09")
        .option("checkpointLocation","/kafka/checkpoint")
      .start().awaitTermination()
  }

}
