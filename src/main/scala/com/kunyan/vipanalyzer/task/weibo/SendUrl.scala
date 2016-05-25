package com.kunyan.vipanalyzer.task.weibo

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.task.moer.UpdateMoerFollowersCount._
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}
import org.apache.log4j.{Level, LogManager}
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON
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

    sendHomePages(lazyConn, sendTopic)

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

      if (result == null || result._1.isEmpty || result._2.isEmpty) {

        lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 0))

      } else {

        val pattern = "(?<=http:\\\\/\\\\/weibo.com\\\\/)(u\\\\/)?\\w+(?=\\?refer_flag)".r

        val userIds = pattern.findAllIn(result._2)

        if (result._2.contains("暂时没有内容哦")) {
          println(url)
          lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))
        }

        while (userIds.hasNext) {

          val userId = userIds.next().replaceAll("\\\\", "")
          val url = "http://weibo.com/" + userId
          val userResult = DBUtil.query(Platform.WEIBO.id.toString, url, lazyConn)

          if (userResult == null) {

            lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

          } else {

            try {

              val jsonPattern = "(?<=<script>FM.view\\()\\{\"ns\":\"pl.header.head.index\".*(?=\\)<\\/script>)".r
              val json = jsonPattern.findFirstIn(userResult._2).getOrElse("")
              val map = JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
              val html = map.get("html").get
              val doc = Jsoup.parse(html)
              val name = doc.getElementsByClass("username").first().text
              val portrait = doc.getElementsByTag("img").first.attr("src")
              val introduction = doc.getElementsByClass("pf_intro").first.text
              val vip = html.contains("key=profile_head&value=vuser_guest")

              val followersCountPattern = "(?<=<script>FM.view\\()\\{\"ns\":\"\",\"domid\":\"Pl_Core_T8CustomTriColumn__3\",.*(?=\\)<\\/script>)".r
              val followersJson = followersCountPattern.findFirstIn(userResult._2).getOrElse("")
              val followersMap = JSON.parseFull(followersJson).get.asInstanceOf[Map[String, String]]
              val followersHtml = followersMap.get("html").get
              val followersDoc = Jsoup.parse(followersHtml)
              val followersCount = followersDoc.getElementsByTag("td").get(1).getElementsByTag("strong").first.text.toInt

              DBUtil.insertWeiboVipInfo(userId, followersCount, vip, name, introduction, url, portrait, lazyConn)

            } catch {

              case e: Exception =>
                lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

            }

          }

        }

      }

    }

  }

}
