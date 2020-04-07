import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
      conf.setAppName("hi")
      conf.setMaster("local[3]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")


    val data: RDD[String] = sc.textFile("F://SogouQ.reduced")
    val value: RDD[(String, Int)] = data.flatMap(_.split("_")).map((_,1)).reduceByKey(_+_)


    val pv: Long = data.count()
    println(pv)


sc.stop()
  }

}
