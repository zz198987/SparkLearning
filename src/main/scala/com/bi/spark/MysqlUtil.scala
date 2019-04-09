package com.bi.spark

import java.sql.DriverManager

object MysqlUtil {
  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.0.168:3306/bigdata","root","rooT_123")
  }

  def main(args: Array[String]): Unit = {
    //val sql = "insert into wc(word,times) values('w',1)"
    val sql = "update wc set times=2 where word='w'"
    val statement = createConnection().createStatement()

    println(statement.executeUpdate(sql))
  }
}
