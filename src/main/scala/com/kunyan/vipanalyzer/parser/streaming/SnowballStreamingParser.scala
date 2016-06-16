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

    val lastUrl = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)
    VALogger.warn("last URL " + lastUrl)

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

              val url = "https://xueqiu.com" + mapInfo.getOrElse("target", "")

              var latestFlag = ""

              val flag = mapInfo.getOrElse("title","")

              if(flag == null){

                val text  = mapInfo.getOrElse("description","").toString

                if (text.length > 30){
                  latestFlag = text.substring(0, 30)
                } else {
                  latestFlag = text
                }

              } else {

                latestFlag = flag.toString.replaceAll("<[^>]*>", "")
              }

              if (i == 0) {

                if (lastUrl != url) {

                  VALogger.warn("url differ")

                  lazyConn.jedisHset(RedisUtil.REDIS_HASH_NAME, pageUrl, url)

                } else {

                  VALogger.warn(pageUrl + "lastUrl: " +lastUrl + "latestURL:  "+url)
                  VALogger.warn("snowball i = 0, break")
                  break()

                }

              }

              if (lastUrl == url) {
                VALogger.warn("lastUrl == url, break")
                break()
              }

              val identifyRetweet = mapInfo.getOrElse("retweeted_status","") //转发标志

              if(identifyRetweet == null){

                val userID = mapInfo.getOrElse("user_id", "")
                val retweet = mapInfo.getOrElse("retweet_count", "").asInstanceOf[Double].toInt
                val reply = mapInfo.getOrElse("reply_count", "").asInstanceOf[Double].toInt
                val timeStamp = new Date().getTime



                VALogger.warn(StringUtil.toJson(Platform.SNOW_BALL.id.toString, 0, url))

                lazyConn.sendTask(topic, StringUtil.toJson(Platform.SNOW_BALL.id.toString, 0, url))

                if(flag != null){

                  val title = flag
                  DBUtil.insertCall(cstmt, userID, title, retweet, reply, url, timeStamp, "")
                }

                VALogger.warn("snowball send task")

              }

            }

          }

        case None => VALogger.error("Parsing failed!")
        case other => VALogger.error("Unknown data structure :" + other)
      }

    } catch {
      case e: Exception =>
        VALogger.exception(e)
        VALogger.warn("snowball "+ pageUrl)
    }

    cstmt.close()
  }

}
