import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //设置程序的名称
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      //设置master
      .master("local[2]")
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    //设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    val inputRdd: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data")
    import spark.implicits._
    inputRdd.toDF
}

}
