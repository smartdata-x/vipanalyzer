package com.kunyan.vipanalyzer.parser

import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup

/**
  * Created by yang on 5/17/16.
  */
object MoerParser {

  val LIST_PREFIX = "http://moer.jiemian.com/investment_findPageList.htm?onColumns=all&industrys=all&fieldColumn=all&price=all&authorType=1&sortType=time&page="
  val USER_PREFIX = "http://moer.jiemian.com/authorHome.htm?theId="
  val HOME_PAGE = "http://moer.jiemian.com/"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    if (url.contains(LIST_PREFIX)) {

      extractUsers(url, html, lazyConn, topic)

    } else {

      extractUserInfo(url, html, lazyConn, topic)

    }

    val message = StringUtil.getUrlJsonString(Platform.MOER.id, url, 1)
    lazyConn.sendTask(topic, message)

  }

  /**
    * 从问答页面提取所有用户的主页地址
    */
  def extractUsers(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.parse(html)

    try {

      val divTags = doc.getElementsByClass("item-info")

      for (i <- 0 until divTags.size) {

        val href = divTags.get(i).getElementsByTag("a").first.attr("href")
        val url = HOME_PAGE + href

        if (!Scheduler.urlSet.contains(url)) {

          Scheduler.urlSet.add(url)

          val result = DBUtil.query(Platform.MOER.id.toString, url, lazyConn)

          if (result == null || result._1.isEmpty || result._2.isEmpty) {
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))
          } else {
            extractUserInfo(result._1, result._2, lazyConn, topic)
          }
        }


      }

      val message = StringUtil.getUrlJsonString(Platform.MOER.id, url, 1)
      lazyConn.sendTask(topic, message)

    } catch {

      case e: Exception =>
        VALogger.warn("Invalid DOM: " + url)

    }

  }

  /**
    * 从用户主页提取用户信息
    */
  def extractUserInfo(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val userId = url.split("theId=")(1)

      val doc = Jsoup.parse(html)
      val portrait = doc.getElementsByClass("author-avatar").first.getElementsByTag("img").attr("src")
      val name = doc.getElementsByClass("author-msg").first.getElementsByTag("span").first.text.trim()

      var vip = 0
      if (doc.getElementsByClass("moerv-blue").size > 0)
        vip = 1
      if (doc.getElementsByClass("moerv-red").size > 0)
        vip = 2

      val field = doc.getElementsByClass("author-msg").first.getElementsByTag("p").first.getElementsByTag("span").text.trim()
      val followersCount = doc.getElementById("fansCount").text.toInt
      val introduction = doc.getElementsByClass("anthor_left_bottom").first.getElementsByTag("tr").get(3).getElementsByTag("p").first.text.trim()

      DBUtil.saveMOERVip(userId, followersCount, vip, name, introduction, url, portrait, field, lazyConn)

      val message = StringUtil.getUrlJsonString(Platform.MOER.id, url, 1)
      lazyConn.sendTask(topic, message)

    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))
        Scheduler.urlSet.remove(url)
        VALogger.warn("Invalid DOM: " + url)

    }

  }

  /**
    * 发送第一批 URL
    */
  def sendFirstPatch(topic: String, lazyConn: LazyConnections): Unit = {

    for (i <- 1 to 2024) {

      val url = LIST_PREFIX + i

      val result = DBUtil.query(Platform.MOER.id.toString, url, lazyConn)

      if (result == null || result._1.isEmpty || result._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))
      } else {
        extractUsers(result._1, result._2, lazyConn, topic)
      }

    }

  }

}
