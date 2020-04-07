import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val conf = new SparkConf()
      conf.setAppName("hi")
      conf.setMaster("local[3]")
      val sparkContext = new SparkContext(conf)
      sparkContext.setLogLevel("WARN")
      sparkContext
    }

    val rdd: RDD[String] = sc.parallelize(List("a", "b", "c", "e", "a", "b", "c", "a", "a"), 2)
    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    val rdd2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7), ("tom", 2)))

    
    
    val tuples: Array[(String, Int)] = rdd1.collect()
    
    
   // val result: (String, Int) = rdd1.first()

//val keys: RDD[String] = rdd1.keys

    //val result: RDD[String] = rdd.distinct()

    //val result: Array[(String, Int)] = rdd1.take(2)
    //val reslut: Long = rdd.count()
     //  val result: RDD[(String, Int)] = rdd1.union(rdd2)





//    val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
 //  result.foreach(println(_))




    sc.stop()

  }
}
