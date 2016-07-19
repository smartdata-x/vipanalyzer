package com.kunyan.vipanalyzer.parser

import java.sql.DriverManager
import java.text.SimpleDateFormat

import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil}
import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser.streaming.TaogubaStreamingParser
import org.apache.log4j.{Level, LogManager}
import org.jsoup.Jsoup

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.xml.Elem

/**
  * Created by yang on 5/15/16.
  */
object TaoGuBaParser {

  val HOME_PAGE = "http://www.taoguba.com.cn/"
  val USER_INFO = "http://www.taoguba.com.cn/getBlogerInfo?userID="
  val FRIEND_LIST = "http://www.taoguba.com.cn/getFriends?userID="
  val BLOG_URL = "http://www.taoguba.com.cn/blog/"
  val IMG_URL = "http://image.taoguba.com.cn/img/"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

//    TaogubaStreamingParser.parse(url, html, lazyConn, topic)
  }

  /**
    * 提取用户列表
    */
  def extractUsers(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    if (Scheduler.urlSet.contains(url))
      return

    try {

      val doc = Jsoup.parse(html)

      if (doc.getElementsByTag("root").first.attr("total") == "0") {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 1))
        return
      }

      val tags = doc.getElementsByTag("keyword")

      for (i <- 0 until tags.size()) {

        val infoUrl = USER_INFO + tags.get(i).attr("userID")

        var result = DBUtil.query(Platform.TAOGUBA.id.toString, infoUrl, lazyConn)

        if (result == null || result._1.isEmpty || result._2.isEmpty) {
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, infoUrl, 0))
        } else {
          extractUserInfo(result._1, result._2, lazyConn, topic)
        }

        val friendsPage = FRIEND_LIST + tags.get(i).attr("userID")
        result = DBUtil.query(Platform.TAOGUBA.id.toString, friendsPage, lazyConn)

        if (result == null || result._1.isEmpty || result._2.isEmpty) {
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, friendsPage, 0))
        } else if (!Scheduler.urlSet.contains(result._1)) {
          Scheduler.urlSet.add(url)
          extractUsers(result._1, result._2, lazyConn, topic)
        }

      }

      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 1))

    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 2))
        VALogger.error("Invalid DOM: " + url)

    }

  }

  /**
    * 提取用户信息存入数据库
    */
  def extractUserInfo(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    if (Scheduler.urlSet.contains(url))
      return

    val doc = Jsoup.parse(html)

    try {

      val json: Option[Any] = JSON.parseFull(doc.body().text)

      if (json.isDefined) {

        //get content from json
        val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

        val userId = map.getOrElse("userID", "")
        val marrow = map.getOrElse("be", "0").toInt
        val recommend = map.getOrElse("us", "0").toInt
        val vip = map.getOrElse("vipAuth", "0").toInt

        DBUtil.insertTGB(userId, (marrow, recommend, vip), lazyConn)

        Scheduler.urlSet.add(url)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 1))

      } else {

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 0))

      }
    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 2))
        VALogger.error("Invalid DOM: " + url)

    }

  }

  /**
    * 发送从主页上爬取的"本周上升达人"作为第一批用户
    *
    * @param lazyConn 连接容器
    * @param topic    要发送的消息的 topic 名
    */
  def sendFirstPatchUsers(lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.connect(HOME_PAGE)
      .userAgent("Mozilla/5.0 (Windows NT 6.1 WOW64) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2")
      .timeout(10000)
      .get()

    val aTags = doc.getElementsByClass("tgb_mr05").first().getElementsByTag("a")

    for (i <- 0 until aTags.size) {

      val userId = aTags.get(i).attr("href").substring(5)

      //parse user info
      val url = USER_INFO + userId

      var result = DBUtil.query(Platform.TAOGUBA.id.toString, url, lazyConn)

      if (result == null || result._1.isEmpty || result._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 0))
      } else {
        extractUserInfo(result._1, result._2, lazyConn, topic)
      }

      //parse users in home page
      val friendsPage = FRIEND_LIST + userId

      result = DBUtil.query(Platform.TAOGUBA.id.toString, friendsPage, lazyConn)

      if (result == null || result._1.isEmpty || result._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, friendsPage, 0))
      } else {
        extractUsers(result._1, result._2, lazyConn, topic)
      }

    }

    for (i <- 1 to 5) {

      val url = "http://www.taoguba.com.cn/viewMoreTop?topFlag=R&pageNo=" + i
      val html = Source.fromURL(url).mkString
      val doc = Jsoup.parse(html)
      val liTags = doc.getElementById("topics").getElementsByTag("ul").first.children()

      for (j <- 0 until liTags.size) {

        val tag = liTags.get(j)
        val userId = tag.getElementsByTag("a").first.attr("href").substring(5)
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, FRIEND_LIST + userId, 0))
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, USER_INFO + userId, 0))
      }

    }
  }

  def sendExistingUrls(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = s"select user_id from user_taoguba"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    while (result.next()) {

      val userId = result.getString("user_id")
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, FRIEND_LIST + userId, 0))
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, USER_INFO + userId, 0))

    }

    statement.close()
    connection.close()

  }

  /**
    * 从 hbase 中提取用户基本信息并存入 mysql, 如果没有改信息则重新爬取
    */
  def saveVipInfo(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = "select a.user_id from temp_taoguba a where (a.recommend > 6 or a.marrow > 0 or a.vip > 0) and a.user_id not in (select b.user_id from vip_taoguba b)"
    val statement = connection.createStatement()
    val data = statement.executeQuery(sql)

    while (data.next()) {

      val userId = data.getString("user_id")
      val url = USER_INFO + userId

      val result = DBUtil.query(Platform.TAOGUBA.id.toString, url, lazyConn)

      if (result == null || result._1.isEmpty || result._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 0))
      } else {
        extractVipInfo(url, result._2, lazyConn, topic)
      }

    }

    statement.close()
    connection.close()

  }

  def extractVipInfo(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val json: Option[Any] = JSON.parseFull(html)

      if (json.isDefined) {

        //get content from json
        val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

        val userId = map.getOrElse("userID", "")
        val followersCount = map.getOrElse("fansNum", "0").toInt
        val vipAuth = map.getOrElse("vipAuth", "0").toInt
        val name = map.getOrElse("userName", "")
        val introduction = map.getOrElse("pe", "")
        val homePage = BLOG_URL + userId
        val portrait = IMG_URL + map.getOrElse("pr", "")
        val marrow = map.getOrElse("be", "0").toInt
        val recommend = map.getOrElse("us", "0").toInt

        DBUtil.saveTGBVip(userId, followersCount, vipAuth, name, introduction, homePage, portrait, marrow, recommend, lazyConn)

      } else {

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 0))

      }
    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 2))
        VALogger.error("Invalid DOM: " + url)

    }

  }

  def saveArticleInfo(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = "select DISTINCT user_id from vip_taoguba"
    val statement = connection.createStatement()
    val data = statement.executeQuery(sql)

    while (data.next()) {

      val userId = data.getString("user_id")
      val url = BLOG_URL + userId

      val result = DBUtil.query(Platform.TAOGUBA.id.toString, url, lazyConn)

      if (result == null || result._1.isEmpty || result._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 2))
      } else {
        extractArticle(url, result._2, lazyConn, topic)
      }

    }

    statement.close()
    connection.close()

  }

  def extractArticle(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val userId = url.split("/").last
      val doc = Jsoup.parse(html)
      val trTags = doc.getElementsByClass("blogtable").first.getElementsByTag("tr")

      for (i <- 1 until trTags.size - 1) {

          val tag = trTags.get(i)
          val tdCount = tag.getElementsByTag("td").size
          val aTag = tag.getElementsByTag("a").first
          val url = HOME_PAGE + aTag.attr("href")
          val title = aTag.attr("title")
          val recommend = tag.getElementsByTag("img").first.attr("src").contains("icon43.gif")
          val countArr = tag.getElementsByTag("td").get(tdCount - 2).text.split("/")
          val readCount = countArr(0).toInt
          val commentCount = countArr(1).toInt
          val date = tag.getElementsByTag("td").get(tdCount - 1).text
          val timeStamp = new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime

          DBUtil.insertTGBArticle(userId, title, recommend, readCount, commentCount, url, timeStamp, lazyConn)


      }

    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 2))
        VALogger.exception(e)

    }

  }

}
