package com.it.like

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

class  SinkToMysql extends ForeachWriter[Row]{

  var conn:Connection=null
  var ps:PreparedStatement=null
  val sql="replace into wcresult(id,word,number) values (null,?,?)"
  override def open(partitionId: Long, version: Long): Boolean = {


    //加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    //构架连接
    conn = DriverManager.getConnection("jdbc:mysql://autumn:3306/test","root","0119")
    //返回true,表示创建成功

    true
  }

  override def process(value: Row): Unit = {
    ps=conn.prepareStatement(sql)
    println("process")
    ps.setString(1,value.getString(0))
    ps.setInt(2,value.getLong(1).toInt)

    ps.executeUpdate()
  }

  override def close(errorOrNull: Throwable): Unit = {

    if(null!=ps) ps.close()
    if(null!=conn) conn.close()
  }
}

