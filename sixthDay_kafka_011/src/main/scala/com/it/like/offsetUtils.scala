package com.it.like

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

object offsetUtils {
  var conn:Connection=null
  {
    if(conn==null){
      Class.forName("com.mysql.jdbc.driver")
     conn= DriverManager.getConnection("jdbc:mysql://autumn:3306/test","root","0119")
    }
  }


  def putOffsetRanges(offsetRanges: Array[OffsetRange], groupId: String)={
val sql=
  """
    |replalce into tb_offset (topic,partition,groupId,offset)values(?,?,?,?)
  """.stripMargin
    val preparedStatement: PreparedStatement = conn.prepareStatement(sql)
    offsetRanges.foreach(range=>{
      preparedStatement.setString(1,range.topic)
      preparedStatement.setInt(2,range.partition)
      preparedStatement.setString(3,groupId)
      preparedStatement.setLong(4,range.untilOffset)
      preparedStatement.addBatch()
    })
    preparedStatement.executeBatch()

  }
  def getOffsetRanges(topics: Set[String],groupId:String):Map[TopicPartition, Long]={
    var map:Map[TopicPartition, Long]=Map.empty
val sql=
  """
    |select partition,offset from tb_offset where topic =? and groupId=?
  """.stripMargin
    val preparedStatement: PreparedStatement = conn.prepareStatement(sql)
    preparedStatement.setString(2,groupId)
    for(topic<-topics){
      preparedStatement.setString(1,topic)
      val resultSet: ResultSet = preparedStatement.executeQuery()
      while(resultSet.next){
        val partition: Int = resultSet.getInt("partition")
        val offset: Long = resultSet.getLong("offset")
        val topicPartition = new TopicPartition(topic,topicPartition)
        map+=topicPartition->offset

      }
    }
    map
  }
}
