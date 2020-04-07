import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val conf = new SparkConf()
      conf.setAppName("hi")
      conf.setMaster("local[3]")
      val sparkContext = new SparkContext(conf)
      sparkContext.setLogLevel("WARN")
      sparkContext
    }

    val sql="insert into wcresult(word,number) values (?,?)"
    var conn:Connection=null
    var ps:PreparedStatement=null
    val result = new JdbcRDD[(String, Int)](
      sc,
      () => {
        val driver = "com.mysql.jdbc.Driver"

        val url = "jdbc:mysql://autumn:3306/test"
        val username = "root"
        val password = "0119"
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      },
      "select word,number from wcresult where id>? and id<?",
      1,
      100,
      1,
      (rs: ResultSet) => {
        val str: String = rs.getString(1)
        val number: Int = rs.getInt(2)
        str -> number
      }


    )
    result.foreach(println(_))
  }

}
