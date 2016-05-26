package com.kunyan.vipanalyzer.parser

import java.sql.DriverManager
import java.text.SimpleDateFormat

import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{DateUtil, DBUtil, StringUtil}
import org.jsoup.Jsoup

import scala.xml.Elem

/**
  * Created by yang on 5/17/16.
  */
object MoerParser {

  val LIST_PREFIX = "http://moer.jiemian.com/investment_findPageList.htm?onColumns=all&industrys=all&fieldColumn=all&price=all&authorType=1&sortType=time&page="
  val USER_PREFIX = "http://moer.jiemian.com/authorHome.htm?theId="
  val ARTICLE_PREFIX = "http://moer.jiemian.com/articleDetails.htm?articleId="
  val HOME_PAGE = "http://moer.jiemian.com/"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    if (url.contains(ARTICLE_PREFIX)) {

      extractArticle(url, html, lazyConn, topic)
      val message = StringUtil.getUrlJsonString(Platform.MOER.id, url, 1)
      lazyConn.sendTask(topic, message)

    } else {

      VALogger.error("Unkonw message: " + url)

    }

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

  def extractArticle(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val doc = Jsoup.parse(html)
      val userId = doc.getElementsByClass("author-avatar").first.attr("href").split("theId=")(1)
      val tag = doc.getElementsByClass("article-container").first
      val title = tag.getElementsByTag("h2").first.text
      val iTags = doc.getElementsByClass("summary-footer").first.getElementsByTag("span").first.getElementsByTag("i")
      val read = iTags.get(0).text.substring(3).replaceAll("次", "").toInt
      val timeStamp = (DateUtil.getTimeStamp(iTags.get(1).text.substring(3), "yyyy年MM月dd日 HH:mm:ss") / 1000).toInt

      var buy = 0

      if (iTags.size > 2)
        buy = iTags.get(2).text.toInt

      var price = 0d
      val priceTag = doc.getElementsByAttributeValue("class", "float-r goOrder").first

      if (priceTag != null)
        price = priceTag.getElementsByTag("strong").first.text.toDouble

      DBUtil.insert(lazyConn, userId, title, read, buy, price, url, timeStamp)

    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))
        VALogger.warn("Invalid DOM: " + url)

    }

  }

  def sendArticleUrl(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    val sql = "select user_id from vip_moer"
    val statement = connection.createStatement()
    val datas = statement.executeQuery(sql)

    while (datas.next) {

      val homePage = USER_PREFIX + datas.getString("user_id")
      val result = DBUtil.query(Platform.MOER.id.toString, homePage, lazyConn)

      if (result == null) {

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, homePage, 2))

      } else {

        val doc = Jsoup.parse(result._2)
        val trTags = doc.getElementById("list1").getElementsByTag("tr")

        for (i <- 0 until trTags.size) {

          val url = HOME_PAGE + trTags.get(i).getElementsByTag("a").first.attr("href")
          val articleResult = DBUtil.query(Platform.MOER.id.toString, url, lazyConn)

          if (articleResult != null) {
            extractArticle(articleResult._1, articleResult._2, lazyConn, topic)
          } else {
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.MOER.id, url, 0))
          }

        }

      }

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
