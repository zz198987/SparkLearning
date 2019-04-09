package com.bi.spark.streaming

import com.bi.spark.MysqlUtil
import com.bi.spark.streaming.Base.getStreamingContext
import org.apache.spark.storage.StorageLevel

/**
  * 将统计结果插入到数据库中
  */
object ForeachRdd {
  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()
    val lines = ssc.socketTextStream("192.168.0.168",6789,StorageLevel.MEMORY_ONLY)
    val result = lines.flatMap(_.split(" ")).map(c=>(c,1)).reduceByKey(_+_)
    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partions => {
        val conn = MysqlUtil.createConnection()
        partions.foreach(record => {
          val updateSql = "update wc set times=times+"+record._2+" where word='"+record._1+"'"
          val statement = conn.createStatement()
          if(1 != statement.executeUpdate(updateSql))
          {
            val insertSql = "insert into wc(word,times) values('"+record._1+"',"+record._2+")"
            if(1 != statement.executeUpdate(insertSql))
            {
              println(s"operation($insertSql) get fail")
            }
          }
        })
        conn.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
