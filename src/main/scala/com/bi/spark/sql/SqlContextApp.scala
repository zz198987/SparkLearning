package com.bi.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlContextApp {

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val sparkConf = new SparkConf()
    //sparkConf.setAppName("SqlContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlcontext = new SQLContext(sc)
    val people = sqlcontext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sc.stop()
  }
}
