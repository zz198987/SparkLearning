package com.bi.spark.sql

import com.bi.spark.BaseT
import org.apache.spark.sql.SparkSession

object Base extends BaseT{
  def getSparkSession():SparkSession= {
    val name = getClass.getSimpleName
    SparkSession.
      builder().
      appName(name.substring(0,name.length-1)).
      master("local[2]").
      getOrCreate()
  }


}
