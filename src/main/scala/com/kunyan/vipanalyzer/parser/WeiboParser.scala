package com.kunyan.vipanalyzer.parser

import java.sql.DriverManager

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.{DateUtil, StringUtil, DBUtil}
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON
import scala.xml.Elem

/**
  * Created by yang on 5/24/16.
  */
object WeiboParser {

  val HOME_PAGE = "http://weibo.com/"

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val userId = url.split("/").last

    val userResult = DBUtil.query(Platform.WEIBO.id.toString, url, lazyConn)

    if (userResult == null) {

      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

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

        insertWeibos(url, html, lazyConn, topic)
      } catch {

        case e: Exception =>
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

      }

    }

  }

  def extractWeibos(configFile: Elem, lazyConn: LazyConnections, topic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    val sql = "select user_id from vip_weibo"
    val statement = connection.createStatement()
    val datas = statement.executeQuery(sql)

    while (datas.next) {

      val url = HOME_PAGE + datas.getString("user_id")
      val result = DBUtil.query(Platform.WEIBO.id.toString, url, lazyConn)

      if (result == null) {

        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

      } else {

        insertWeibos(result._1, result._2, lazyConn, topic)

      }

    }
  }

  def insertWeibos(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val userId = url.split("/").last

    val userResult = DBUtil.query(Platform.WEIBO.id.toString, url, lazyConn)

    if (userResult == null) {

      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

    } else {

      try {

        val jsonPattern = "(?<=<script>FM.view\\()\\{\"ns\":\"pl.content.homeFeed.index\",\"domid\":\"Pl_Official_MyProfileFeed.*(?=\\)<\\/script>)".r
        val json = jsonPattern.findFirstIn(userResult._2).getOrElse("")
        val map = JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
        val html = map.get("html").get
        val doc = Jsoup.parse(html)

        val divTags = doc.getElementsByClass("WB_detail")

        for (i <- 0 until divTags.size) {

          val tag = divTags.get(i)
          val postUrl = HOME_PAGE.substring(0, HOME_PAGE.length - 1) + tag.getElementsByTag("div").get(2).getElementsByTag("a").first.attr("href")
          val timeStamp = (DateUtil.getTimeStamp(tag.getElementsByTag("div").get(2).getElementsByTag("a").first.attr("title"), "yyyy-MM-dd HH:mm") / 1000).toInt

          val countEle = tag.parent.nextElementSibling().getElementsByTag("ul").first
          val retweet = countEle.getElementsByTag("li").get(1).getElementsByTag("em").get(1).text.toInt
          val reply = countEle.getElementsByTag("li").get(2).getElementsByTag("em").get(1).text.toInt
          val like = countEle.getElementsByTag("li").get(3).getElementsByTag("em").get(0).text.toInt

          DBUtil.insert(lazyConn, userId, "", retweet, reply, like, postUrl, timeStamp)
        }

      } catch {

        case e: Exception =>
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

      }

    }

  }

}
