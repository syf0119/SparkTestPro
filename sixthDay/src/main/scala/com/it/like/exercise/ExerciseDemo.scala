package com.it.like.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//
//1.统计网站pv
//page view 网站页面浏览量(访问一次算一次)
//2.统计网站uv
//user view 独立用户访问量(一个用户算一次访问,可以使用ip/Sessionid)
//3.统计网站用户来源topN
//统计refurl,分析用户表示来自于哪里

object ExerciseDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession .builder().master("local[4]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
   val data: RDD[String] = sparkSession.sparkContext.textFile("F://access.log")
  val filterRdd: RDD[String] = data.filter(line=>line!=null&&line.trim.split(" ").size>0)
  filterRdd.persist()
sparkSession.sparkContext.setCheckpointDir("./data")
    filterRdd.checkpoint()

    val pv: Long = filterRdd.count()
    println("pv:"+pv)

   val uv: Long = filterRdd.map(_.split(" ")(0)).distinct().count()
    println("uv:"+uv)

    val refurl: Array[(String, Int)] = filterRdd
      .filter(line=>line.split(" ").size > 10&&line.split(" ")(10).length>3)
      .map(_.split(" ")(10) -> 1)
      .reduceByKey(_ + _)
      .sortBy(_._2, false).take(10)
println("...................top10.....................")
    refurl.foreach(println)








    sparkSession.stop()

  }

}
