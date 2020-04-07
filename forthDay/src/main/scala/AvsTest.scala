import org.apache.spark.sql.{DataFrame, SparkSession}

object AvsTest {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello")
      .master("local[2]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("warn")
val dataJson: DataFrame = sparkSession.read.format("json").load("forthDay/src/main/datas/resources/people.json")
    import org.apache.spark.sql.functions._
    val toLowCase = udf(
      (name:String) =>  name.toLowerCase
    )
    import sparkSession.implicits._
    val result : DataFrame = dataJson.select($"name",toLowCase($"name"))
    result.show()
    sparkSession.stop()
  }

}
