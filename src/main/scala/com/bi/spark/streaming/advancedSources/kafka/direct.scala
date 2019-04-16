package com.bi.spark.streaming.advancedSources.kafka
import com.bi.spark.streaming.Base._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils


object direct {
  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.0.168:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
