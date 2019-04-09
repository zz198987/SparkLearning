package com.bi.spark.streaming

import com.bi.spark.streaming.Base.getStreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time

object Stream2Sql {
  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()
    val lines = ssc.socketTextStream("192.168.0.168",6789,StorageLevel.MEMORY_ONLY)
    lines.flatMap(_.split(" ")).
      foreachRDD{
        (rdd:RDD[String],time:Time) =>{
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
          import spark.implicits._

          // Convert RDD[String] to RDD[case class] to DataFrame
          val wordsDataFrame = rdd.map(w => Record(w)).toDF()

          // Creates a temporary view using the DataFrame
          wordsDataFrame.createOrReplaceTempView("words")

          // Do word count on table using SQL and print it
          val wordCountsDataFrame =
            spark.sql("select word, count(*) as total from words group by word")
          println(s"========= $time =========")
          wordCountsDataFrame.show()
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

case class Record(word: String)