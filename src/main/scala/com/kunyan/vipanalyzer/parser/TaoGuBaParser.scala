package com.kunyan.vipanalyzer.parser

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON

/**
  * Created by yang on 5/15/16.
  */
object TaoGuBaParser {

  val HOME_PAGE = "http://www.taoguba.com.cn/"
  val USER_INFO = "http://www.taoguba.com.cn/getBlogerInfo?userID="

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    if (url.contains(USER_INFO)) {

      extractUserInfo(url, html, lazyConn, topic)

    } else {

      val doc = Jsoup.parse(html)
      val result = doc.getElementById("pop_cont_4").getElementsByTag("a")

      for (i <- 0 until result.size()) {

        val url = USER_INFO + result.get(i).attr("href").substring(5)

        val message = StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 0)
        lazyConn.sendTask(topic, message)

        val homePage = HOME_PAGE + result.get(i).attr("href")
        lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, homePage, 0))

      }

    }

    val message = StringUtil.getUrlJsonString(Platform.TAOGUBA.id, url, 1)
    lazyConn.sendTask(topic, message)
  }

  /**
    * 提取用户信息存入数据库
    */
  def extractUserInfo(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    val doc = Jsoup.parse(html)
    val json: Option[Any] = JSON.parseFull(doc.body().text)

    if (json.isDefined) {

      //get content from json
      val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

      val userId = map.getOrElse("userID", "")
      val marrow = map.getOrElse("be", "0").toInt
      val recommend = map.getOrElse("us", "0").toInt
      val vip = map.getOrElse("vipAuth", "0").toBoolean

      DBUtil.insertTGB(userId, (marrow, recommend, vip), lazyConn)
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

    val result = doc.getElementsByClass("tgb_mr05").first().getElementsByTag("a")

    for (i <- 0 until result.size) {

      val userInfo = USER_INFO + result.get(i).attr("href").substring(5)
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, userInfo, 0))

      val homePage = HOME_PAGE + result.get(i).attr("href")
      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.TAOGUBA.id, homePage, 0))

    }

  }

}
