package com.kunyan.vipanalyzer.parser

import java.sql.DriverManager

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}
import org.apache.log4j.{Level, LogManager}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.xml.Elem

/**
  * Created by yang on 5/12/16.
  * 中金博客解析类
  */
object CNFOLParser {

  val ARTICLE_URL = "http://blog.cnfol.com/index.php/article/blogarticlelist/"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.parse(html)
    var result = ""

    if (url.endsWith("friend")) {

      try {

        result = doc.getElementsByClass("page").first().getElementsByClass("CoRed").first().text.trim
        val totalPage = result.split("/")(1).toInt

        for (i <- 2 to totalPage) {
          val message = StringUtil.getUrlJsonString(Platform.CNFOL.id, url + "?p=" + i.toString, 0)
          lazyConn.sendTask(topic, message)
        }

      } catch {
        case e: Exception =>
          val message = StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 1)
          lazyConn.sendTask(topic, message)
      }

    }

    val pTags = doc.getElementsByClass("FollowInfo")

    for (i <- 0 until pTags.size()) {

      val p = pTags.get(i)
      val href = p.getElementsByTag("a").first().attr("href")

      if (!href.contains("returnbolg")) {

        val followersCount = p.getElementsByTag("a").get(1).getElementsByTag("em").text.trim

        if (followersCount.nonEmpty) {

          if (followersCount.toInt > 100) {

            val userId = href.split("/myfocus/friend")(0).split(".com/")(1)
            DBUtil.insertCNFOL(userId, followersCount.toInt, lazyConn)

            val message = StringUtil.getUrlJsonString(Platform.CNFOL.id, href, 0)
            lazyConn.sendTask(topic, message)

          }

        }

      }

    }

    val message = StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 1)
    lazyConn.sendTask(topic, message)
  }

  /**
    * 发送从主页上爬取的第一批用户
    *
    * @param lazyConn 连接容器
    * @param topic    要发送的消息的 topic 名
    */
  def sendFirstPatchUsers(lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.connect("http://blog.cnfol.com/")
      .userAgent("Mozilla/5.0 (Windows NT 6.1 WOW64) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2")
      .timeout(10000)
      .get()

    var result = doc.getElementById("gowell").getElementsByClass("user")

    for (i <- 0 until result.size) {

      val url = result.get(i).attr("href") + "/myfocus/friend"
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))

    }

    result = doc.getElementsByClass("BlogMUl BlockStyle1 Ml30 Fl Mt10").first().getElementsByTag("li")

    for (i <- 0 until result.size) {

      val url = result.get(i).getElementsByTag("a").attr("href") + "/myfocus/friend"
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))

    }

    result = doc.getElementById("goStock").getElementsByTag("li")

    for (i <- 0 until result.size) {

      val url = result.get(i).getElementsByTag("a").last().attr("href") + "/myfocus/friend"
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))

    }

  }

  /**
    * 检查用户是否为官方认证vip
    */
  def checkOfficialVip(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.parse(html)
    val vipTagCount = doc.getElementsByClass("ApproveIco").size()

    if (vipTagCount > 0) {

      val userId = url.substring(22)
     VALogger.warn(userId)

    }

    lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 1))
  }

  /**
    * 从个人博客主页解析用户信息
    */
  def parseHomePage(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val doc = Jsoup.parse(html)

      val id = url.substring(22)
      val name = doc.getElementsByClass("BloggerName").first().getElementsByTag("strong").text()
      val introduction = doc.getElementById("BlogNameLoad").text().trim
      val vip = doc.getElementsByClass("ApproveIco").size() > 0

      DBUtil.saveCNFOLVip(id, vip, name, introduction, url, lazyConn)

     VALogger.warn(s"ID: $id, NAME: $name, INTRO: $introduction, VIP: $vip")
    } catch {

      case e: Exception =>
        e.printStackTrace()
       VALogger.warn(s"Invalid data: $url")

    }

  }

  def sendVipHomePage(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = "select * from user_cnfol where followers_count>500 or vip=TRUE group by user_id"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    while (result.next()) {

      val userId = result.getString("user_id")
      val url = s"http://blog.cnfol.com/$userId"
      val hbaseResult = DBUtil.query(Platform.CNFOL.id.toString, url, lazyConn)

      if (hbaseResult == null || hbaseResult._1.isEmpty || hbaseResult._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))
      } else {
        parseHomePage(hbaseResult._1, hbaseResult._2, lazyConn, topic)
      }

    }

    statement.close()
    connection.close()
  }

  def sendBlogPages(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    VALogger.warn((configFile \ "mysql" \ "url").text + (configFile \ "mysql" \ "username").text + (configFile \ "mysql" \ "password").text)
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = "select * from vip_cnfol"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    while (result.next()) {

      val userId = result.getString("user_id")
      val url = ARTICLE_URL + userId
      val hbaseResult = DBUtil.query(Platform.CNFOL.id.toString, url, lazyConn)

      if (hbaseResult == null || hbaseResult._1.isEmpty || hbaseResult._2.isEmpty) {
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))
      } else {
        parseBlog(hbaseResult._1, hbaseResult._2, lazyConn, topic)
      }

    }

    statement.close()
    connection.close()
  }

  /**
    * 解析博客
    */
  def parseBlog(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.parse(html)

    if (url.contains("?page=")) {

      extractBlogUrl(doc, lazyConn, topic)
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 1))

    } else if (url.contains("blogarticlelist")) {

      try {

        var number = 1
        if (doc.getElementsByClass("CoRed").size() > 0) {
          number = doc.getElementsByClass("CoRed").first().text().trim().substring(2).toInt
        }

        for (i <- 2 to number) {

          val pageUrl = url + "?page=" + i
          val hbaseResult = DBUtil.query(Platform.CNFOL.id.toString, pageUrl, lazyConn)

          if (hbaseResult == null || hbaseResult._1.isEmpty || hbaseResult._2.isEmpty) {
            lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, pageUrl, 0))
          } else {
            extractBlogUrl(Jsoup.parse(hbaseResult._2), lazyConn, topic)
          }

        }

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 1))
      } catch {
        case e: Exception =>
          e.printStackTrace()
         VALogger.warn("Page with invalid DOM: " + url)
      }

    } else {

      extractArticleInfo(url, doc, lazyConn, topic)

    }

  }

  /**
    * 提取页面中的博客 url
    */
  def extractBlogUrl(doc: Document, lazyConn: LazyConnections, topic: String): Unit = {

    val tags = doc.getElementsByClass("ArtTit")

    for (i <- 0 until tags.size()) {

      val url = tags.get(i).attr("href")
      val time = tags.get(i).nextElementSibling().text

      if (time.contains("2016-05")) {

        val hbaseResult = DBUtil.query(Platform.CNFOL.id.toString, url, lazyConn)

        if (hbaseResult == null || hbaseResult._1.isEmpty || hbaseResult._2.isEmpty) {
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))
        } else {
          extractArticleInfo(url, Jsoup.parse(hbaseResult._2), lazyConn, topic)
        }

      }

    }

  }

  /**
    * 提取文章信息
    */
  def extractArticleInfo(url: String, doc: Document, lazyConn: LazyConnections, topic: String): Unit = {

    try {

      val userId = url.split("/article/")(0).substring(22)
      val title = doc.getElementsByAttributeValueContaining("class", "ArticleBox NewArtbox").first.getElementsByTag("h1").first.getElementsByTag("a").text
      val recommend = doc.getElementsByClass("Recom").size() > 0
      val time = doc.getElementsByClass("MBTime").first.ownText().trim()

      if (time.contains("2016-05")) {

        val reproduce = doc.getElementById("transshipmentnum").text.toInt
        val comment = doc.getElementById("ArticleCommentNum").text.toInt

        DBUtil.saveBlogInfo(userId, title, recommend, time, reproduce, comment, url, lazyConn)
      }

    } catch {

      case e: Exception =>
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))
        VALogger.error("Invalid blog page: " + url)

    }

  }

}
