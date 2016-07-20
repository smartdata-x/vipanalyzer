package com.kunyan.vipanalyzer.task.common

import java.sql.DriverManager
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.parser.article.SnowBallContent
import com.kunyan.vipanalyzer.util.DBUtil

import scala.xml.XML

/**
  * Created by yangshuai on 2016/6/5.
  */
object AddHistorySummary {

  def main(args: Array[String]): Unit = {

    updateSummary(args(0))
  }

  def updateSummary(path: String): Unit = {

    val configFile = XML.loadFile(path)

    val lazyConn = LazyConnections(configFile)

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    val sql = s"select url from article_info where summary is null"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    val updateSummaryPs = connection.prepareStatement("update article_info set digest=?, summary=? where url=?")

    while (result.next()) {

      val url = result.getString("url")
      val content = DBUtil.queryContent("vip_article", url, lazyConn)

      if (content != null) {
        println(content)
      }

    }

    statement.close()
    updateSummaryPs.close()
    connection.close()

  }

}
