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

          case Some(mapInfo: Map[String, AnyVal]) =>

            val newHtml = mapInfo.get("html").get.toString
            val newDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_cardwrap WB_feed_type S_bg2")
            val anotherDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_cardwrap WB_feed_type S_bg2 WB_feed_vipcover")
            //两个URL对比
            val lastURL = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)
            val latestURL = getLatestUrl(newDoc, anotherDoc)
            VALogger.warn("lasteURL" + lastURL + "latestURL" + latestURL)
            var div = anotherDoc.get(0)

            breakable {

              for (i <- 0 until newDoc.size + anotherDoc.size()) {

                if (i < anotherDoc.size) {

                  div = anotherDoc.get(i)

                } else if (i >= anotherDoc.size) {

                  div = newDoc.get(i - anotherDoc.size)

                }

                if (i == 0) {

                  if (lastURL != latestURL) {
                    VALogger.warn("weibo url differ")
                    lazyConn.jedisHset(RedisUtil.REDIS_HASH_NAME, pageUrl, latestURL)
                    VALogger.warn(pageUrl + "lastURL: " + lastURL + "latestURL:  " + latestURL)
                    VALogger.warn("weibo i = 0, break")
                  } else {
                    break()
                  }

                }

                if (lastURL == latestURL) {
                  VALogger.warn("lastURL == latestURL, break")
                  break()
                }

                val children = div.getElementsByAttributeValue("class", "WB_detail").get(0)
                val expand = children.select("div.WB_feed_expand")

                if (expand.isEmpty) {

                  val date = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("title")
                  val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                  val timeStamp = fm.parse(date).getTime
                  val url = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("href")

                  if (!url.contains("?ref=home&rid=")) {
                    break()
                  }

                  val getUrl = url.split("ref=home")(0)
                  val totalUrl = "http://weibo.com" + getUrl + "type=comment"

                  var result = "" //用户发的文章标题

                  if (children.toString.contains("WB_media_wrap clearfix")) {

                    var title = children.getElementsByAttributeValue("class", "WB_media_wrap clearfix").get(0)

                    if (title != null && title.toString.contains("WB_feed_spec_info SW_fun")) {

                      title = title.getElementsByAttributeValue("class", "WB_feed_spec_info SW_fun").get(0)

                      if (title != null && title.toString.contains("WB_feed_spec_cont")) {
                        result = title.getElementsByAttributeValue("class", "WB_feed_spec_cont").select("h4").text()
                      }

                    }

                  }

                  //                  var content = children.getElementsByAttributeValue("node-type", "feed_list_content").get(0).text()
                  //
                  //                  if (content.length >= 30) {
                  //                    content = content.substring(0, 30)
                  //                  }

                  var user = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").text()
                  val userCard = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").attr("usercard")
                  val userId = userCard.split("id=")(1).split("&")(0)
                  val botChild = div.getElementsByAttributeValue("class", "WB_feed_handle")
                  val forward = botChild.select("li span.pos em")
                  var forwardContent = ""

                  if (forward.size >= 4) {
                    forwardContent = forward.get(3).text()
                  } else {
                    forwardContent = 0.toString
                  }

                  if (forwardContent.startsWith("转发")) {
                    forwardContent = 0.toString
                  }

                  val repeat = botChild.select("li span.pos em")
                  var repeatContent = ""

                  if (repeat.size >= 6) {
                    repeatContent = repeat.get(5).text()
                  } else {
                    repeatContent = 0.toString
                  }

                  if (repeatContent.startsWith("评论")) {
                    repeatContent = 0.toString
                  }

                  val like = botChild.select("li span.pos em")
                  var likeContent = ""

                  if (like.size >= 7) {
                    likeContent = like.get(6).text()
                  } else {
                    likeContent = 0.toString
                  }

                  if (likeContent.startsWith("赞")) {
                    likeContent = 0.toString
                  }

                  if (user.isEmpty) {
                    user = children.getElementsByAttributeValue("class", "W_fb S_txt1").text()
                  }

                  VALogger.warn("this is weibo" + totalUrl)
                  VALogger.warn(StringUtil.toJson(Platform.WEIBO.id.toString, 1, totalUrl))

                  DBUtil.insertCall(cstmt, userId, result, forwardContent.toInt, repeatContent.toInt, likeContent.toInt, totalUrl, timeStamp, "")
                  lazyConn.sendTask(topic, StringUtil.toJson(Platform.WEIBO.id.toString, 1, totalUrl))
                }

              }

            }

          case None => VALogger.error("Parsing failed!")
          case other => VALogger.error("Unknown data structure :" + other)
        }

      }

    } catch {
      case e: Exception =>
        VALogger.exception(e)
    }

    cstmt.close()
  }

  /**
    * 获取页面时间最新的链接
    *
    * @param newDoc     提取页面信息
    * @param anotherDoc 提取页面信息
    * @return 返回链接字符串
    */
  def getLatestUrl(newDoc: Elements, anotherDoc: Elements): String = {

    var index = 0
    var lastTime = 0L
    var div = anotherDoc.get(0)

    for (i <- 0 until newDoc.size + anotherDoc.size()) {

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

    val getUrl = url.split("ref=home")(0)
    val totalUrl = "http://weibo.com" + getUrl + "type=comment"

    totalUrl
  }

}
