import org.apache.spark.sql.{DataFrame, SparkSession}

object UDFTest {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello")
      .master("local[2]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("warn")
    sparkSession.udf.register("toLowerCase",(str:String)=>{
      str.toLowerCase
    })
    val frame: DataFrame = sparkSession.read.format("json").load("forthDay/src/main/datas/resources/people.json")
    frame.createOrReplaceTempView("view_person")
    val sql=
      """
        |select name,toLowerCase(name) as lwname from view_person
      """.stripMargin
    val ResultTable: DataFrame = sparkSession.sql(sql)
//    table.printSchema()
//    table.show()
    ResultTable.printSchema()
    ResultTable.show()
    sparkSession.stop()

  }

}
