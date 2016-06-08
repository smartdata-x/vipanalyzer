package com.kunyan.vipanalyzer.parser.streaming

import java.text.SimpleDateFormat
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil, RedisUtil}
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import scala.util.control.Breaks._
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/6/8.
  */
object WeiboStreamingParser {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    val doc = Jsoup.parse(html, "UTF-8")

    val cstmt = lazyConn.mysqlConn.prepareCall("{call proc_InsertWeiboNewArticle(?,?,?,?,?,?,?,?)}")

    val lastTitle = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)

    try {

      var index = 0

      for (i <- 0 until doc.getElementsByTag("script").size()) {

        if (doc.getElementsByTag("script").get(i).toString.contains("pl.content.homefeed.index")) {
          index = i
        }

      }

      val docStr = doc.getElementsByTag("script").get(index).toString
      val result = docStr.substring(16, docStr.length - 10)
      val jsonInfo = JSON.parseFull(result)

      if (jsonInfo.isEmpty) {

        VALogger.error("\"JSON parse value is empty,please have a check!\"")

      } else {
        jsonInfo match {

          case Some(mapInfo: Map[String, AnyVal]) => {

            val newHtml = mapInfo.get("html").get.toString
            val newDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_cardwrap WB_feed_type S_bg2")
            val anotherDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_cardwrap WB_feed_type S_bg2 WB_feed_vipcover")
            //两个URL对比
            val lastURL = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)
            val latestURL = getLatestUrl(newDoc, anotherDoc)

            breakable {

              for (i <- 0 until newDoc.size + anotherDoc.size()) {

                var div = anotherDoc.get(0)

                if (i < anotherDoc.size) {

                  div = anotherDoc.get(i)

                } else if (i >= anotherDoc.size) {

                  div = newDoc.get(i - anotherDoc.size)

                }

                if (i == 0) {

                  if (lastURL != latestURL) {
                    lazyConn.jedisHset(RedisUtil.REDIS_HASH_NAME, pageUrl, latestURL)
                  } else {
                    break()
                  }

                }

                if (lastURL == latestURL) {
                  break()
                }

                val children = div.getElementsByAttributeValue("class", "WB_detail").get(0)
                val date = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("title")
                val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                val timeStamp = fm.parse(date).getTime
                val url = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("href")
                val totalUrl = "http://weibo.com" + url + "&type=comment"
                var content = children.getElementsByAttributeValue("node-type", "feed_list_content").get(0).text()

                if (content.length >= 30) {
                  content = content.substring(0, 30)
                }

                var user = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").text()
                val userCard = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").attr("usercard")
                val userId = userCard.split("id=")(1).split("&")(0)
                val botChild = div.getElementsByAttributeValue("class", "WB_feed_handle")
                var forwardContent = botChild.select("li span.pos em").get(3).text

                if (forwardContent.startsWith("转发")) {
                  forwardContent = 0.toString
                }

                val repeatContent = botChild.select("li span.pos em").get(5).text
                val likeContent = botChild.select("li span.pos em").get(6).text

                if (user.isEmpty) {
                  user = children.getElementsByAttributeValue("class", "W_fb S_txt1").text()
                }

                DBUtil.insertCall(cstmt, userId, content, forwardContent.toInt, repeatContent.toInt, likeContent.toInt, totalUrl, timeStamp, "")
                lazyConn.sendTask(topic, StringUtil.toJson(Platform.WEIBO.id.toString, totalUrl))
              }

            }

          }

          case None => VALogger.error("Parsing failed!")
          case other => VALogger.error("Unknown data structure :" + other)
        }

      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }

    cstmt.close()
  }

  /**
    * 获取页面时间最新的链接
    * @param newDoc  提取页面信息
    * @param anotherDoc 提取页面信息
    * @return 返回链接字符串
    */
  def getLatestUrl(newDoc: Elements, anotherDoc: Elements): String = {

    var index = 0
    var lastTime = 0L

    for (i <- 0 until newDoc.size + anotherDoc.size()) {

      var div = anotherDoc.get(0)

      if (i < anotherDoc.size) {

        div = anotherDoc.get(i)
      } else if (i >= anotherDoc.size) {
        div = newDoc.get(i - anotherDoc.size)

      }

      val children = div.getElementsByAttributeValue("class", "WB_detail").get(0)
      val date = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("title")
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
      val timeStamp = fm.parse(date).getTime

      if (timeStamp > lastTime) {
        lastTime = timeStamp
        index = i
      }

    }

    var child = anotherDoc.get(0)

    if (index < anotherDoc.size) {

      child = anotherDoc.get(index)
    } else if (index >= anotherDoc.size) {
      child = newDoc.get(index - anotherDoc.size)

    }

    val children = child.getElementsByAttributeValue("class", "WB_detail").get(0)

    val url = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("href")

    val totalUrl = "http://weibo.com" + url + "&type=comment"

    totalUrl
  }

}
