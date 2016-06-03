package com.kunyan.vipanalyzer.task.content

import java.sql.Connection

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.DBUtil
import org.apache.hadoop.hbase.client.Table

/**
  * Created by niujiaojiao on 2016/6/3.
  * 中金博客数据库操作
  */
object CnfolContent {
  /**
    * 读取相应的信息
    *
    * @param lazyConn   连接配置信息
    * @param connection 数据库连接配置信息
    * @param table      表名
    */
  def get(lazyConn: LazyConnections, connection: Connection, table: Table): Unit = {

    val cnfolSql = s"select url from article_cnfol where  summary != ''"
    val statement = connection.createStatement()
    val cnfolResult = statement.executeQuery(cnfolSql)

    while (cnfolResult.next()) {

      val url = cnfolResult.getString("url")
      val content = DBUtil.query(Platform.CNFOL.id.toString, url, lazyConn)

      if (content != null) {
        ContentUtil.putResultToTable(table, url, content._2)
      }

    }

    statement.close()
  }

}
