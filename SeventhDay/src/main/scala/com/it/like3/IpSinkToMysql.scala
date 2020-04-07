package com.it.like3

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

class  IpSinkToMysql extends ForeachWriter[Row]{

  var conn:Connection=null
  var ps:PreparedStatement=null
  val sql="replace into tb_iplocation(longitude,longitude,latitude) values (?,?,?)"
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

    ps.setString(1,value.getString(0))
    ps.setString(2,value.getString(1))
    ps.setLong(3,value.getLong(2))

    ps.executeUpdate()
  }

  override def close(errorOrNull: Throwable): Unit = {

    if(null!=ps) ps.close()
    if(null!=conn) conn.close()
  }
}

