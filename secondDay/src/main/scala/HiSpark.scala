import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

object HiSpark {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val conf = new SparkConf()
      conf.setAppName("hi")
      conf.setMaster("local[1]")
      val sparkContext = new SparkContext(conf)
      sparkContext.setLogLevel("WARN")
      sparkContext
    }

    val data: RDD[String] = sc.textFile("F:/input")
    //data.repartition(1)
    val filterRdd: RDD[String] = data.filter(line => {
      line.replaceAll(" ", "").length != 0
    })
    val trimRdd: RDD[String] = filterRdd.map(_.trim)
    //trimRdd.foreach(println(_))
    val flatMapRdd: RDD[String] = trimRdd.flatMap(_.split(" "))
    val mapRdd: RDD[String] = flatMapRdd.map(_.replaceAll("[^a-zA-Z0-9]",""))
    val tupleRdd: RDD[(String, Int)] = mapRdd.map(_->1)
    val reduceRdd: RDD[(String, Int)] = tupleRdd.reduceByKey(_+_)
    val sortRdd: RDD[(String, Int)] = reduceRdd.sortBy(-_._2)
    sortRdd.foreach(println(_))
    Thread.sleep(1000000)








    //    val partitions1: Array[Partition] = sc.makeRDD(List(5,6,4,7,3,8,2,9,1,10)).partitions
    //    val partitions2: Array[Partition] = sc.parallelize(List(5,6,4,7,3,8,2,9,1,10),3).partitions
    //    println(partitions1.length)
    //    println(partitions2.length)

    //    val partitions: Array[Partition] = data.partitions
    //    println(partitions.length)

    sc.stop()
  }

}
