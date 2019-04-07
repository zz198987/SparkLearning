package com.bi.spark.sql

import Base._

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    //将json文件加载成为一个dataFrame
    val peopleDF = spark.read.format("json").load(getFilePath("people.json"))

    //打印scema到控制台
    peopleDF.printSchema()
    //打印内容
    peopleDF.show()
    println("--------------------")
    peopleDF.select(peopleDF.col("name"),peopleDF.col("age")+10).show()

    println("--------------------")
    peopleDF.filter(peopleDF.col("age") > 19).show()
  }
}
