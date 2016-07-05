package com.kunyan.vipanalyzer.task.cnfol

import java.sql.DriverManager

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.DBUtil
import org.apache.log4j.{Level, LogManager}
import org.jsoup.Jsoup

import scala.xml.XML

/**
  * Created by yang on 5/17/16.
  */
object UpdatePortrait {

  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile(args(0))

    val lazyConn = LazyConnections(configFile)

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val selectUserId = s"select user_id from vip_cnfol;"
    val selectStatement = connection.createStatement()
    val result = selectStatement.executeQuery(selectUserId)

    val updateStatement = connection.prepareStatement("update vip_cnfol set portrait=? where user_id=?")

    while (result.next()) {

      val userId = result.getString("user_id")
      val url = s"http://blog.cnfol.com/$userId"

      try {

        val result = DBUtil.query(Platform.CNFOL.id.toString, url, lazyConn)

        if (result == null) {

          VALogger.warn(s"Invalid rowkey: $url")

        } else {

          val content = result._2
          val doc = Jsoup.parse(content)
          val portrait = doc.getElementsByClass("BloggerPic").first.getElementsByTag("img").first.attr("src")

          updateStatement.setString(1, portrait)
          updateStatement.setString(2, userId)
          updateStatement.executeUpdate()
        }

      } catch {

        case e: Exception =>
          VALogger.exception(e)
          VALogger.warn(s"Invalid html with userId: $userId")

      }

    }

    selectStatement.close()
    updateStatement.close()
    connection.close()
  }

}
