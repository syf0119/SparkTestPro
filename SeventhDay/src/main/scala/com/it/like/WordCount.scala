package com.it.like


import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql._

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello")
      .master("local[2]")
  .config("spark.sql.shuffle.partitions","2")
      .getOrCreate()
   sparkSession.sparkContext.setLogLevel("warn")
    val dataFrame: DataFrame = sparkSession.readStream.option("host", "autumn")
      .option("port", "9999")
      .format("socket")
      .load()
    import sparkSession.implicits._
   val dataSet: Dataset[String] = dataFrame.as[String]
    val rsDataFrame: DataFrame = dataSet.filter(line => null != line && line.trim.length > 0)

      .flatMap(_.trim.split(" "))
      .groupBy($"value")
      .count()

   rsDataFrame.writeStream
      .outputMode(OutputMode.Complete())
        .foreach(new   SinkToMysql())

      .start()
      .awaitTermination()


//    rsDataFrame.writeStream
//      .outputMode(OutputMode.Complete())
//        .format("console")
//      .start()
//.awaitTermination()


  }

}
