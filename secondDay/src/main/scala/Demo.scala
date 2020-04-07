import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val conf = new SparkConf()
      conf.setAppName("hi")
      conf.setMaster("local[3]")
      val sparkContext = new SparkContext(conf)
      sparkContext.setLogLevel("WARN")
      sparkContext
    }

    val pairRdd = sc.parallelize(Array(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = pairRdd.groupBy(_._1)
    val result: RDD[(String, Int)] = groupRdd.map(line => {
      val tuple: (String, Int) = line._2.reduce((x, y) => x._1 -> (x._2 + y._2))
      val count: Int = line._2.size
      line._1 -> (tuple._2 / count)
    }
    )
    result.foreach(println(_))
    Thread.sleep(1000000)


  }

}
