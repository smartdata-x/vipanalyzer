package com.kunyan.vipanalyzer.task.content

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import java.sql.Connection
import com.kunyan.vipanalyzer.util.DBUtil
import org.apache.hadoop.hbase.client.Table

/**
  * Created by niujiaojiao on 2016/6/3.
  * 摩尔金融数据库操作
  */
object MoerContent {
  /**
    * 读取相应的信息
    *
    * @param lazyConn   连接配置信息
    * @param connection 数据库连接配置信息
    * @param table      表名
    */
  def get(lazyConn: LazyConnections, connection: Connection, table: Table): Unit = {

    val moerSql = s"select url from article_moer where summary != ''"
    val statement = connection.createStatement()
    val moerResullt = statement.executeQuery(moerSql)

    while (moerResullt.next()) {

      val url = moerResullt.getString("url")
      val content = DBUtil.query(Platform.MOER.id.toString, url, lazyConn)

      if (content != null) {
        ContentUtil.putResultToTable(table, url, content._2)
      }

    }

    statement.close()
  }

}
