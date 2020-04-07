package com.it.like

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object StructSourceJson {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("hello")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val structType: StructType = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)
      .add("hobby", StringType)





    val dataFrame: DataFrame = sparkSession.readStream.schema(structType).json("SeventhDay/data/")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val rsDataset : Dataset[Row] = dataFrame.filter($"age" < 25)
      .groupBy($"hobby")
         .agg(
           count($"hobby").as("cnt")
         )

      .orderBy($"cnt".desc)


    /**
      * 需求：
1-读取Json数据
2-统计25以下的人的兴趣排名
      */

    rsDataset.writeStream
        .outputMode(OutputMode.Complete())
      .format("console")
      .option("truncate",false)
      .option("numRows",10)
      .start()
      .awaitTermination()

  }

}
