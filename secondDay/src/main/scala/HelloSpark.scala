import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val conf: SparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("helloSpark")
      val sparkContext: SparkContext = SparkContext.getOrCreate(conf)
      sparkContext

    }
    sc.setLogLevel("ERROR")
    //sc.textFile("file:///F:\\input")
    val list = List("a", "b", "c", "e", "a", "b", "c", "a", "a")
    val rdd: RDD[String] = sc.parallelize(list, 2)
    val mapRdd: RDD[(String, Int)] = rdd.map(_ -> 1)
    //val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupBy(_._1)
    //    val result: RDD[(String, Int)] = groupRdd.map(x => {
    //
    //      val tuple: (String, Int) = x._2.reduce((re, ar) => (re._1 -> (re._2 + re._2)))
    //      tuple
    //    })

    // groupRdd.foreach(print)

    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey((x, y) => x + y)
    reduceRdd.foreach(println(_))
    println("+++++++++++++++")
    //    val sortRDD: RDD[(String, Int)] = reduceRdd.sortBy(_._2)
    val sortRDD: RDD[(String, Int)] = reduceRdd.sortByKey(false)
    val result: Array[(String, Int)] = sortRDD.take(2)
    //


    result.foreach(println(_))
    //Thread.sleep(1000000)

    sc.stop()

  }

}
