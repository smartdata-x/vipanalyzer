package com.kunyan.vipanalyzer.task.content

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import java.sql.Connection
import com.kunyan.vipanalyzer.util.DBUtil
import org.apache.hadoop.hbase.client.Table

/**
  * Created by niujiaojiao on 2016/6/3.
  * 雪球数据库操作
  */
object SnowballContent {
  /**
    * 读取相应的信息
    *
    * @param lazyConn   连接配置信息
    * @param connection 数据库连接配置信息
    * @param table      表名
    */
  def get(lazyConn: LazyConnections, connection: Connection, table: Table): Unit = {

    val snowballSql = s"select url from article_snowball where summary != ''"
    val statement = connection.createStatement()
    val snowballResult = statement.executeQuery(snowballSql)

    while (snowballResult.next()) {

      val url = snowballResult.getString("url")
      val content = DBUtil.query(Platform.SNOW_BALL.id.toString, url, lazyConn)

      if (content != null) {
        ContentUtil.putResultToTable(table, url, content._2)
      }
    }

    statement.close()
  }

}
