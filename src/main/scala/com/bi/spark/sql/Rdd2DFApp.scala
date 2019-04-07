package com.bi.spark.sql

import Base.{getFilePath, getSparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Rdd2DFApp {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession();

    val rdd = spark.sparkContext.textFile(getFilePath("info.txt"))
      //method1
    //innerReflection(spark,rdd)

    //method2
    program(spark,rdd)

    spark.stop()
  }
  private def program(spark:SparkSession,rdd: RDD[String]) = {
        val rddMap = rdd.map(_.split(",")).map(line => Row(line(0).toInt,line(1),line(2).toInt))
        val structType=StructType(Array(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("age",IntegerType,true)))
    val infoDf = spark.createDataFrame(rddMap,structType)
    infoDf.printSchema()
    infoDf.show
  }

  private def innerReflection(spark:SparkSession,rdd: RDD[String]) = {
    //inner refection
    import spark.implicits._
    val infos = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infos.printSchema()
    infos.show()
  }

  case class Info(id:Int, name:String, age:Int)
}
