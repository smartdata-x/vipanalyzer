package com.kunyan.vipanalyzer.parser.streaming

import java.util.Date
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil, RedisUtil}
import org.jsoup.Jsoup
import scala.util.control.Breaks._
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/6/3.
  * 雪球
  */
object SnowballStreamingParser {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    val doc = Jsoup.parse(html, "UTF-8")

    val cstmt = lazyConn.mysqlConn.prepareCall("{call proc_InsertSnowBallNewArticle(?,?,?,?,?,?,?)}")

    val lastTitle = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)

    try {

      var index = 0

      for (i <- 0 until doc.getElementsByTag("script").size()) {

        if (doc.getElementsByTag("script").get(i).toString.contains("SNB.data.statuses =")) {
          index = i
        }

      }

      val contentStr = doc.getElementsByTag("script").get(index).toString.split("SNB.data.statusType =")(0).split("SNB.data.statuses =")(1).trim

      val resultContent = contentStr.substring(0, contentStr.length - 1)

      val jsonInfo = JSON.parseFull(resultContent)

      if (jsonInfo.isEmpty) {

        VALogger.error("\"JSON parse value is empty,please have a check!\"")

      } else jsonInfo match {

        case Some(mapInfo: Map[String, List[Map[String, Any]]]) =>

          val content = mapInfo.getOrElse("statuses", "").asInstanceOf[List[Map[String, Any]]]

          breakable {

            for (i <- content.indices) {

              val mapInfo = content(i)
              var title = mapInfo.getOrElse("text", "").toString

              if (title.length >= 30) {
                title = title.substring(0, 30)
              }

              if (i == 0) {

                if (lastTitle != title) {
                  lazyConn.jedisHset(RedisUtil.REDIS_HASH_NAME, pageUrl, title)
                } else {
                  break()
                }

              }

              if (lastTitle == title) {
                break()
              }

              val userID = mapInfo.getOrElse("user_id", "")
              val retweet = mapInfo.getOrElse("retweet_count", "").asInstanceOf[Double].toInt
              val reply = mapInfo.getOrElse("reply_count", "").asInstanceOf[Double].toInt
              val timeStamp = new Date().getTime
              val url = "https://xueqiu.com" + mapInfo.getOrElse("target", "")

              DBUtil.insertCall(cstmt, userID, title, retweet, reply, url, timeStamp, "")
              lazyConn.sendTask(topic, StringUtil.toJson(Platform.SNOW_BALL.id.toString, url))

            }

          }
        case None => VALogger.error("Parsing failed!")
        case other => VALogger.error("Unknown data structure :" + other)
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    cstmt.close()
  }

}
