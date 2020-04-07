package com.it.like.exercise

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ExerciseBySQL {
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
    val result: DataFrame = mapDataSet.select($"_1".as("course"), $"_2".as("name"))
  result.show()
    result.createOrReplaceTempView("view")
    val sql=
      """select course,name,cnt from
        |(select course,name,cnt ,row_number() over(partition by course order by cnt desc) as rn_num
        |from
        |(select course,name,count(1) as cnt from view
        |group by course,name))
        |where rn_num=1
      """.stripMargin
val finalResult: DataFrame = sparkSession.sql(sql)
    finalResult.show()

  }

}
