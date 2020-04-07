import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BySchema {
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
    val schema = new StructType(
      Array(
         StructField("userid", StringType, false),
         StructField("itermid", StringType, false),
         StructField("rate", DoubleType, false),
         StructField("time", LongType, false)
      )
    )
    val dataFrame: DataFrame = sparkSession.createDataFrame(rowRdd,schema)

    //dataFrame.printSchema()
   // dataFrame.show()
//
//    import sparkSession.implicits._
//   val dataSet: Dataset[MovieRate] = dataFrame.as[MovieRate]
//   dataSet.printSchema()
//   dataSet.show()

   dataFrame.createOrReplaceTempView("view_movie_top10")
   val sql=
     """
       |select itermid,avg(rate) as score,count(1) as cnt_num from view_movie_top10
       |group by itermid
       |having count(userid)>2000
       |order by score desc,cnt_num desc
       |limit 10
     """.stripMargin

val resultTable: DataFrame = sparkSession.sql(sql)
    resultTable.show()

sparkSession.close()
  }

}