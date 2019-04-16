package com.bi.spark

import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


object HbaseT1Dao {
  def batchPut(lists:List[T1])={
    val table = new HbaseConn().getInstance.getTable(TableName.valueOf("t1"))
    val htable = table.asInstanceOf[HTable]
    htable.setAutoFlush(false,false)
    val putList:util.List[Put]= new util.ArrayList[Put]()
    for(e <- lists)
    {
      var put:Put  = new Put(Bytes.toBytes(e.rowKey))
      put.addColumn(Bytes.toBytes(e.f2),Bytes.toBytes(e.qualifier),Bytes.toBytes(e.value))
      putList.add(put)
     }
    htable.put(putList)
    htable.flushCommits()
    htable.close()
  }

  def batchIncr(lists:ListBuffer[T1])={
    val table = new HbaseConn().getInstance.getTable(TableName.valueOf("t1"))
    for(e <- lists)
    {
      table.incrementColumnValue(Bytes.toBytes(e.rowKey),Bytes.toBytes(e.f2),Bytes.toBytes(e.qualifier),e.value)
    }
    table.close()
  }
  def batchIncr(lists:ListBuffer[T1],conn:Connection)={
    val table = conn.getTable(TableName.valueOf("t1"))
    for(e <- lists)
    {
      table.incrementColumnValue(Bytes.toBytes(e.rowKey),Bytes.toBytes(e.f2),Bytes.toBytes(e.qualifier),e.value)
    }
    table.close()
  }

  def main(args: Array[String]): Unit = {
    val list:ListBuffer[T1] = new ListBuffer[T1]()
    list+=new T1("hh","f2","count",11l)
    batchIncr(list)

  }
}

case class T1(rowKey:String,f2:String="f2",qualifier:String="count",value:Long);
