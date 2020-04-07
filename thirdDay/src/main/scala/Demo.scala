import java.sql.{Connection, DriverManager, PreparedStatement}

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
    val rdd: RDD[String] = sc.textFile("F://input")
    val result: RDD[(String, Int)] = rdd.filter(_.replaceAll(" ", "").length != 0)
      .flatMap(_.split(" "))
      .map(_.replaceAll("[^a-zA-Z]", "") -> 1)
      .reduceByKey(_ + _)
    batchInsert(result)


sc.stop()
  }
  def batchInsert(result:RDD[(String, Int)])={
    val driver="com.mysql.jdbc.Driver"

    val url="jdbc:mysql://autumn:3306/test"
    val username="root"
    val password="0119"
    val sql="insert into wcresult(word,number) values (?,?)"
    var conn:Connection=null
    var ps:PreparedStatement=null

    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, username, password)
      ps = conn.prepareStatement(sql)
      val tuples: Array[(String, Int)] = result.collect()
      for(tuple<-tuples){
        ps.setString(1, tuple._1)
        ps.setInt(2, tuple._2)
        ps.addBatch()
      }
      ps.executeBatch()
    } catch{
      case e:Exception=>e.printStackTrace()
    }finally{
      if(ps!=null) ps.close()
      if(conn!=null) conn.close()
    }


  }

  def hello(func:(String)=>Int)={

  }
}