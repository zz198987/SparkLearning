package com.bi.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseConn1 {
  def getConn:Connection = {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum","192.168.0.168")
      conf.set("hbase.zookeeper.property.clientPort","2181")
       ConnectionFactory.createConnection(conf);
  }
}
