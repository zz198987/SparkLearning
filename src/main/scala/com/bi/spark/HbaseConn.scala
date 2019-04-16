package com.bi.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

class HbaseConn {

  private var instance: Connection = null

  def getInstance:Connection = {
    if(null == instance)
      {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum","192.168.0.168")
        conf.set("hbase.zookeeper.property.clientPort","2181")
        instance = ConnectionFactory.createConnection(conf);
      }
      instance
  }
}
object HbaseConn
{
  def main(args: Array[String]): Unit = {

    val connection = new HbaseConn().getInstance;
    val table = connection.getTable(TableName.valueOf("t1"))
    table.incrementColumnValue(Bytes.toBytes("cc"),Bytes.toBytes("f1"),Bytes.toBytes("cc"),10)
    // list the tables

  }
}

