package com.it.like.exercise

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ExerciseByDSL {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[4]")
      .appName("test").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dataSet: Dataset[String] = sparkSession.read.textFile("F://data")
    val mapDataSet: Dataset[(String, String)] = dataSet.map(line => {
      val info: String = line.split("//")(1)
      val course: String = info.split("/")(0).split("\\.")(0)
      val name: String = info.split("/")(1)
      course -> name

    })
    import org.apache.spark.sql.functions._
    val result: DataFrame = mapDataSet.select($"_1".as("course"), $"_2".as("name"))
    val r1: DataFrame= result.select($"course", $"name")
      .groupBy($"course", $"name")
      .agg(
        count($"name").as("cnName")

      )
    val r2: DataFrame = r1
      .select($"course", $"name", $"cnName")
      .groupBy($"course")
      .agg(
        max($"cnName").as("cnName")
      )
r1.show()
    r2.show()


    r2.join(r1,Seq("course","cnName"),"left").show()




    sparkSession.stop()
  }

}
