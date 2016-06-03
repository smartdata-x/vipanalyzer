package com.kunyan.vipanalyzer.task.content

import com.kunyan.vipanalyzer.db.LazyConnections
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}

/**
  * Created by niujiaojiao on 2016/6/3.
  * 函数类
  */
object ContentUtil {

  val COLUMN_FAMILY_NAME = "basic"

  val FAMILY_NAME = "content"

  /**
    * 创建hbase表
    *
    * @param tableName 表名
    * @param families  列簇名
    * @param lazyConn  服务器连接配置信息
    */
  def createHbaseTable(tableName: TableName, families: List[String], lazyConn: LazyConnections): Unit = {

    val admin = lazyConn.hbaseConn.getAdmin()
    val htd = new HTableDescriptor(tableName)

    for (family <- families) {
      val hcd = new HColumnDescriptor(family)
      htd.addFamily(hcd.setMaxVersions(3))
    }

    admin.createTable(htd)
    admin.close()
  }

  /**
    * 结果写入hbase表中
    *
    * @param table   表名
    * @param url     表的rowkey
    * @param content 将要写入的内容
    */
  def putResultToTable(table: Table, url: String, content: String): Unit = {

    val resultPut = new Put(Bytes.toBytes(url))
    resultPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(content))
    table.put(resultPut)
  }

  /**
    * 清空table 里面数据
    *
    * @param tableName 要清空的hbase表名的字符串
    * @param lazyConn  kafka,redis,hbase的服务端的连接配置信息
    */
  def emptyHbaseTable(tableName: TableName, lazyConn: LazyConnections): Unit = {

    val admin = lazyConn.hbaseConn.getAdmin
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    admin.close()
  }

}
