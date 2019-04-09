package com.bi.spark.streaming

import com.bi.spark.BaseT
import com.bi.spark.sql.Base.getClass
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Base extends BaseT{
  def getStreamingContext():StreamingContext ={
    val name = getClass.getSimpleName
    val sparkConf =new SparkConf().setMaster("local[2]").setAppName(name.substring(0,name.length-1))
    new StreamingContext(sparkConf,Seconds(5))
  }
}
