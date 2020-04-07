import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object DSLTest {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession .builder()
      .appName("hello")
      .master("local[2]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val rddData: RDD[String] = sparkSession.sparkContext.textFile("forthDay/src/main/datas/ml-1m/ratings.dat")
    val rowRdd: RDD[Row] = rddData.filter(line=>line.split("::").size==4)map(line => {
      val Array(userid, itermid, rate, time) = line.split("::")
      Row(userid, itermid, rate.toDouble, time.toLong)
    })
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val schema = new StructType(
      Array(
        new StructField("userid", StringType, false),
        new StructField("itermid", StringType, false),
        new StructField("rate", DoubleType, false),
        new StructField("time", LongType, false)
      )
    )
    val dataFrame: DataFrame = sparkSession.createDataFrame(rowRdd,schema)
    val ds: DataFrame = dataFrame
      .select($"itermid")
      .groupBy($"itermid")
        .agg(
          avg($"rate").as("avg_rate"),

          count($"itermid").as("cnt_num")

        )
        .filter($"cnt_num">2000)

        .orderBy($"avg_rate".desc,$"cnt_num".desc)
      .limit(10)
    ds.printSchema()
    ds.show()

  }
}
