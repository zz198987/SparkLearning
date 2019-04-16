package com.bi.spark.streaming.advancedSources.kafka


import com.bi.spark.{HbaseConn1, HbaseT1Dao, T1}
import com.bi.spark.streaming.Base._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ListBuffer


object Direct01 {
  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "47.104.137.172:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val words = lines.map(_.split("\t"))
      .filter(x =>{x(2).contains("class")})
      .map(x => {
      var s = x(1)
      var content = x(2)
      s = s.substring(0, 4) + s.substring(5, 7) + s.substring(8, 10)
      var index = content.indexOf(".")
      content = content.substring(12,index)
      (s.concat("_").concat(content),1)
    })
      .foreachRDD(rdd => {
        rdd.foreachPartition( f = partions => {
          val conn = HbaseConn1.getConn
          var lists: ListBuffer[T1] = new ListBuffer[T1]()
          partions.foreach(record => {
            lists+=(new T1(record._1, "f2","count",record._2.toLong))
          })
          HbaseT1Dao.batchIncr(lists,conn)
          conn.close()

        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
