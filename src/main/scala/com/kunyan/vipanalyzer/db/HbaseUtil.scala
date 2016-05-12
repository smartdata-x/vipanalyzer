package com.kunyan.vipanalyzer.db

import com.kunyan.vipanalyzer.logger.VALogger
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by yangshuai on 2016/5/11.
  *
  * @author yangshuai
  *         数据库相关操作
  */
object HbaseUtil {

  def getHTMLBytes(tableName: String, rowkey: String, connections: LazyConnections): Array[Byte] = {

    val connection = connections.hbaseConnection
    val hTable = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowkey))
    get.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("content"))

    val result = hTable.get(get)
    if (result == null) {
      VALogger.error(s"该 rowkey 没有对应的数据: ($tableName, $rowkey)")
      return null
    }

    result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

  }

}
