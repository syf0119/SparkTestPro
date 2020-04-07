import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TestTran {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello")
      .master("local[2]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val rddDataPerson: RDD[Person] = sparkSession.sparkContext.parallelize(List(new Person("zs",18),new Person("zs",18),new Person("zs",18)))
    val rddData: RDD[Int] = sparkSession.sparkContext.parallelize(List(1,3,2))
   import sparkSession.implicits._






    val dataFrame: DataFrame = rddData.toDF()
    val dataSet: Dataset[Int] = rddData.toDS()
    dataFrame.printSchema()
    dataFrame.show()



    sparkSession.stop()
  }

}
class Person(name:String,age:Int){



}
