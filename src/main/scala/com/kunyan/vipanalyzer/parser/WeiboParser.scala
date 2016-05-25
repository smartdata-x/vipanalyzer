package com.kunyan.vipanalyzer.parser

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil}
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON

/**
  * Created by yang on 5/24/16.
  */
object WeiboParser {

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

      } catch {

        case e: Exception =>
          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.WEIBO.id, url, 2))

      }

    }
  }

}
