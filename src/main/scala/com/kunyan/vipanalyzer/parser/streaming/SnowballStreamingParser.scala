package com.kunyan.vipanalyzer.parser.streaming

import java.util.Date

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil, RedisUtil}
import org.jsoup.Jsoup

import scala.util.control.Breaks._

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
    val list = doc.select("div.mod div#status-list div.status-item")

    val cstmt = lazyConn.mysqlConn.prepareCall("{call proc_InsertCNFOLNewArticle(?,?,?,?,?,?,?,?)}")

    val lastTitle = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)
    val timeStamp = new Date().getTime

    breakable {

      for (i <- 0 until list.size) {

        val child = list.get(i)
        val title = child.select("div.UserBox a.Tit").text()

        if (i == 0) {

          if (title != lastTitle) {
            lazyConn.jedisHset(RedisUtil.REDIS_HASH_NAME, pageUrl, title)
          } else {
            break()
          }

        }


        if (title == lastTitle)
          break()

        try {

          val child = list.get(i)
          val userId = child.select("div.status-bd h4 a").attr("href").substring(1)
          val content = child.select("div.status-bd  div.status-content div.detail  div.summary").text()
          val time = child.select("div.status-ft div.status-meta  div.meta-info a.time").text()
          val title = child.select("div.status-bd div.status-content h4.status-title  a").text()

//          DBUtil.insertCall(cstmt, userId, title, recommended, reproduce, comment, url, timeStamp, "")
//
//          lazyConn.sendTask(topic, StringUtil.toJson(Platform.CNFOL.id.toString, url))

        } catch {
          case e: Exception =>
            VALogger.exception(e)
        }

      }

    }

    cstmt.close()
  }

}
