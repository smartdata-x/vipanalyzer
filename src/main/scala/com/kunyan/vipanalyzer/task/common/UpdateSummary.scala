package com.kunyan.vipanalyzer.task.common

import java.sql.DriverManager

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.parser.article.{TaoGuBaContent, MoerContent, CNFOContent, SnowBallContent}
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}

import scala.xml.XML

/**
  * Created by yangshuai on 2016/5/29.
  */
object UpdateSummary {

  def main(args: Array[String]): Unit = {

    updateWeibo(args(0))

  }

  def updateWeibo(path: String): Unit = {

    val configFile = XML.loadFile(path)

    val lazyConn = LazyConnections(configFile)

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    val sql = s"select * from (select * from article_weibo order by ts desc limit 3000) a where a.summary is null"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    val updateSummaryPs = connection.prepareStatement("update article_weibo set summary=? where url=?")

    while (result.next()) {

      val url = result.getString("url")
      val hbaseResult = DBUtil.query(Platform.WEIBO.id.toString, url, lazyConn)

      if (hbaseResult == null) {

        lazyConn.sendTask((configFile \ "kafka" \ "send").text, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

      } else {

        val html = hbaseResult._2
        val results = TaoGuBaContent.getContent(html)

        if (results == null) {

          lazyConn.sendTask((configFile \ "kafka" \ "send").text, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

        } else {

          var content = results._2

          if (content.length > 200)
            content = content.substring(0, 200)

          updateSummaryPs.setString(1, content)
          updateSummaryPs.setString(2, url)
          updateSummaryPs.executeUpdate()
        }

      }

    }

    statement.close()
    updateSummaryPs.close()
    connection.close()
  }


  def updateSnowBall(path: String): Unit = {

    val configFile = XML.loadFile(path)

    val lazyConn = LazyConnections(configFile)

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    val sql = s"select * from (select * from article_snowball order by ts desc limit 3000) a where a.summary is null"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    val updateSummaryPs = connection.prepareStatement("update article_snowball set summary=? where url=?")

    while (result.next()) {

      val url = result.getString("url")
      val hbaseResult = DBUtil.query(Platform.CNFOL.id.toString, url, lazyConn)

      if (hbaseResult == null) {

        lazyConn.sendTask((configFile \ "kafka" \ "send").text, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, url, 2))

      } else {

        val html = hbaseResult._2
        var content = SnowBallContent.getContent(html)._2

        if (content.length > 300)
          content = content.substring(0, 300)

        updateSummaryPs.setString(1, content)
        updateSummaryPs.setString(2, url)
        updateSummaryPs.executeUpdate()
      }

    }

    statement.close()
    updateSummaryPs.close()
    connection.close()
  }
}
