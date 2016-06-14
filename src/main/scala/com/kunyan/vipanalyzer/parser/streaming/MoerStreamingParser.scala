package com.kunyan.vipanalyzer.parser.streaming

import java.util.Date
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil, RedisUtil}
import org.jsoup.Jsoup
import scala.util.control.Breaks._

/**
  * Created by niujiaojiao on 2016/6/7.
  * 摩尔金融
  */
object MoerStreamingParser {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(pageUrl: String, html: String, lazyConn: LazyConnections, topic: String) = {

    val doc = Jsoup.parse(html, "UTF-8")
    val list = doc.getElementsByAttributeValue("class", "blu authortab-list").select("tr")

    val cstmt = lazyConn.mysqlConn.prepareCall("{call proc_InsertMoerNewArticle(?,?,?,?,?,?,?,?)}")

    val lastTitle = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)
    val timeStamp = new Date().getTime / 1000

    breakable {

      for (i <- 0 until list.size) {

        val child = list.get(i)
        val title = child.select("a").get(0).text()

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

          val userId = pageUrl.split("theId=")(1)
          val title = list.get(i).select("a").get(0).text()
          val read = 0
          val buy = 0
          val price = 0.0
          val url = "http://moer.jiemian.com/" + list.get(i).select("a").get(0).attr("href")
          val stock = ""

          VALogger.info(StringUtil.toJson(Platform.MOER.id.toString, 0, url))

          DBUtil.insertCall(cstmt, userId, title, read, buy, price, url, timeStamp, stock)
          lazyConn.sendTask(topic, StringUtil.toJson(Platform.MOER.id.toString, 0, url))

        } catch {
          case e: Exception =>
            VALogger.exception(e)
        }

      }

    }

    cstmt.close()
  }

}
