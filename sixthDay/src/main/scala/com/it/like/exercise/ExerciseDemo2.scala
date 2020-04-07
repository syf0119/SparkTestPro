package com.it.like.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//1.在所有的老师中求出最受欢迎的老师Top1
//2.求每个学科中最受欢迎老师的Top1

object ExerciseDemo2 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[4]")
      .appName("test").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")

    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("F://urls.log")
    dataRdd.foreach(println)

    val mapData: RDD[(String, String)] = dataRdd.filter(line => line.contains("/") && line.contains("//") && line.contains("."))
      .map(line => {
        val course = line.split("//")(1).split("/")(0)
    val name: String = line.split("//")(1).split("/")(1)
    course.split("\\.")(0) -> name
  })
    val result1: (String, Int) = mapData.map(_._2->1).reduceByKey(_+_).sortBy(_._2,false).first()
    println(result1)

    mapData.groupBy(_._1).foreach(
      tuple=>{

        val groupTUple: Map[String, Iterable[(String, Int)]] = tuple._2.map(_._2->1).groupBy(_._1)
        val reduceTuple: Map[String, Int] = groupTUple.map(tuple2 => {
          tuple2._2.reduce((x, y) => x._1 -> (x._2 + y._2))
        })
        val resTuple: (String, Int) = reduceTuple.maxBy(_._2)
        println(" 学科 ："+tuple._1+"  最受欢迎老师："+resTuple._1+"  欢迎指数："+resTuple._2)
      }
    )



    sparkSession.close()


  }

}
