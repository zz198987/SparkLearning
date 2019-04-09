package com.bi.spark.streaming

import com.bi.spark.streaming.Base.getStreamingContext
import org.apache.spark.storage.StorageLevel

/**
  * nc -lk 6789
  */
object socketStreamFilterBlacks {

  def main(args: Array[String]): Unit = {
    val ssc = getStreamingContext()
    val lines = ssc.socketTextStream("192.168.0.168",6789,StorageLevel.MEMORY_ONLY)
    val blacks = List("zs","ls")
    val blackRdd = ssc.sparkContext.parallelize(blacks).map(x=> (x,true))
    lines.map(x=>(x.split(" ")(1),x)).transform(rdd =>{
      rdd.leftOuterJoin(blackRdd).
        filter(r => r._2._2.getOrElse(false) != true).
        map(x =>x._2._1)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
