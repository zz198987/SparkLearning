package com.bi.spark.streaming

import Base._
import org.apache.spark.storage.StorageLevel
object socketStream {

  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()
    val lines = ssc.socketTextStream("192.168.0.168",6789,StorageLevel.MEMORY_ONLY)
    lines.flatMap(_.split(" ")).map(c=>(c,1)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
