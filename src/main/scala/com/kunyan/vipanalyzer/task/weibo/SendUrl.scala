package com.kunyan.vipanalyzer.task.weibo

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.task.moer.UpdateMoerFollowersCount._
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}
import org.apache.log4j.{Level, LogManager}
import org.jsoup.Jsoup

import scala.xml.XML

/**
  * Created by yang on 5/19/16.
  */
object SendUrl {

  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile(args(0))
    val sendTopic = (configFile \ "kafka" \ "send").text

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val lazyConn = LazyConnections(configFile)

    sendList(lazyConn, sendTopic)

  }

  def sendList(lazyConn: LazyConnections, sendTopic: String): Unit = {

    val lazyConn = LazyConnections(configFile)

    for (i <- 1 to 21) {
      val url = s"http://d.weibo.com/230771_-_EXPERTUSER?page=$i"
      lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 0))
    }

  }

  def sendHomePages(lazyConn: LazyConnections, sendTopic: String): Unit = {

    for (i <- 1 to 21) {

      val url = s"http://d.weibo.com/230771_-_EXPERTUSER?page=$i"
      val result = DBUtil.query(Platform.WEIBO.id.toString, url, lazyConn)

      println(url)
      if (result == null || result._1.isEmpty || result._2.isEmpty) {

        lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 0))

      } else {

        val pattern = "(?<=http:\\\\/\\\\/weibo.com\\\\/)(u\\\\/)?\\w+(?=\\?refer_flag)".r

        val userIds = pattern.findAllIn(result._2)

        while (userIds.hasNext) {

          val userId = userIds.next().replaceAll("\\\\", "")
          val url = "http://weibo.com/" + userId
          println(url)
          lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 0))

        }

      }
    }

  }

}
