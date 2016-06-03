package com.kunyan.vipanalyzer.task.content

import java.sql.DriverManager
import com.kunyan.vipanalyzer.db.LazyConnections
import org.apache.hadoop.hbase.TableName
import org.apache.log4j.{Level, LogManager}
import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/6/2.
  * 操作数据库，把读取匹配数据写入hbase表
  */
object Content extends App {

  val TABLE_NAME = "vip_article"

  val COLUMN_FAMILY_NAME = "basic"

  val FAMILY_NAME = "content"

  val configFile = XML.loadFile(args(0))
  val lazyConn = LazyConnections(configFile)
  Class.forName("com.mysql.jdbc.Driver")
  val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

  sys.addShutdownHook {
    connection.close()
  }

  LogManager.getRootLogger.setLevel(Level.WARN)

  val hbaseTableName = TableName.valueOf(TABLE_NAME)

  if (!lazyConn.hbaseConn.getAdmin.tableExists(hbaseTableName)) {

    ContentUtil.createHbaseTable(hbaseTableName, List(COLUMN_FAMILY_NAME), lazyConn)

  } else {

    ContentUtil.emptyHbaseTable(TableName.valueOf(TABLE_NAME), lazyConn)
    ContentUtil.createHbaseTable(hbaseTableName, List(COLUMN_FAMILY_NAME), lazyConn)
  }

  val table = lazyConn.getTable(TABLE_NAME)

  CnfolContent.get(lazyConn, connection, table)
  MoerContent.get(lazyConn, connection, table)
  SnowballContent.get(lazyConn, connection, table)
  TaogubaContent.get(lazyConn, connection, table)

  connection.close()

}
