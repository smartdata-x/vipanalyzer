package com.kunyan.vipanalyzer.task.content

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.DBUtil
import java.sql.Connection
import org.apache.hadoop.hbase.client.Table

/**
  * Created by niujiaojiao on 2016/6/3.
  * 淘股吧数据库操作
  */
object TaogubaContent {
  /**
    * 读取相应的信息
    *
    * @param lazyConn   连接配置信息
    * @param connection 数据库连接配置信息
    * @param table      表名
    */
  def get(lazyConn: LazyConnections, connection: Connection, table: Table): Unit = {

    val taogubaSql = s"select url from article_taoguba where summary != ''"
    val statement = connection.createStatement()
    val taogubaResult = statement.executeQuery(taogubaSql)

    while (taogubaResult.next()) {

      val url = taogubaResult.getString("url")
      val content = DBUtil.query(Platform.TAOGUBA.id.toString, url, lazyConn)

      if (content != null) {
        ContentUtil.putResultToTable(table, url, content._2)
      }

    }

    statement.close()
  }

}
