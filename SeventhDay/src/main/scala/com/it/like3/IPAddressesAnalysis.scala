package com.it.like3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IPAddressesAnalysis {
  /**
    * 通过日志信息（运行商或者网站自己生成）和城市ip段信息来判断用户的ip段，统计热点经纬度
    *		a. 加载IP地址信息库，获取起始IP地址和结束IP地址Long类型值及对应经度和维度
    *		b. 读取日志数据，提取IP地址，将其转换为Long类型的值
    *		c. 依据Ip地址Long类型值获取对应经度和维度 - 二分查找
    *		d. 按照经度和维分组聚合统计出现的次数，并将结果保存到MySQL数据库中
    */
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("hello")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val userDataFrame: DataFrame = sparkSession.read.text("SeventhDay/data/ips/20090121000132.394251.http.format")
    val ipDataFrame: DataFrame = sparkSession.read.text("SeventhDay/data/ips/ip.txt")
import  sparkSession.implicits._
    val ipDataSet: Dataset[(Long, Long, String, String)] = ipDataFrame.as[String].filter(line => line != null && line.trim.length > 0)
      .mapPartitions(part => {
        part.map(line => {
          val strings: Array[String] = line.split("\\|")
          (strings(2).toLong, strings(3).toLong, strings(strings.length - 2), strings(strings.length - 1))
        })
      })
    val arr: Array[(Long, Long, String, String)] = ipDataSet.collect()
//    println(arr.length)
//    val ipNum: Long = IPtoLong("125.213.100.123")
//    println(ipNum)
//val tuple: (String, String) = BinarySearch.binarySearch(ipNum,arr)
//    println(tuple)

    import sparkSession.implicits._
val broadcast: Broadcast[Array[(Long, Long, String, String)]] = sparkSession.sparkContext.broadcast(arr)

    val userDataSet: Dataset[String] = userDataFrame.as[String]

    val tupleDs: Dataset[(String, String)] = userDataSet.filter(line => null != line && line.trim.split("\\|").length>2)
      .mapPartitions(part => {
        part.map(line => {
          val strings: Array[String] = line.split("\\|")
          val ipNum: Long = IPtoLong(strings(1))
          BinarySearch.binarySearch(ipNum, broadcast.value)


        })
      })

//    tupleDs.printSchema()
//    tupleDs.show(20,false)

    val rs  : DataFrame = tupleDs.toDF("longitude", "latitude")
      .groupBy($"longitude", $"latitude")
      .count()
      .orderBy($"count".desc)
    rs.printSchema()
    rs.show()


       sparkSession.stop()






  }
  def IPtoLong(ip:String): Long={
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

}
