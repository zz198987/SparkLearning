package com.bi.spark.streaming

import com.bi.spark.streaming.Base.getStreamingContext
import Base._

object textFileStream {
  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()
    val lines = ssc.textFileStream("hdfs://192.168.0.168:9000/sparkdir")
    lines.flatMap(_.split(" ")).map(c=>(c,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
