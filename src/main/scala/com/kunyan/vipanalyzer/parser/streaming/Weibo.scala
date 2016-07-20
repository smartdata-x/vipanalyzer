package com.kunyan.vipanalyzer.parser.streaming

import com.kunyan.vipanalyzer.logger.VALogger
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/6/6.
  * 微博
  */
object Weibo {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(html: String) = {

    val doc = Jsoup.parse(html, "UTF-8")

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
      var value = ""

      if (jsonInfo.isEmpty) {

        println("\"JSON parse value is empty,please have a check!\"")

      } else {
        jsonInfo match {

          case Some(mapInfo) => {

            val newHtml = mapInfo.asInstanceOf[Map[String, AnyVal]].get("html").get.toString
            val newDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_detail")

            for (i <- 0 until newDoc.size) {

              val children = newDoc.get(i)
              val date = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("title")
              val content = children.getElementsByAttributeValue("node-type", "feed_list_content").get(0).text()
              var user = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").text()

              if (user.isEmpty) {
                user = children.getElementsByAttributeValue("class", "W_fb S_txt1").text()
              }

              (user, content, date)
            }

          }

          case None => println("Parsing failed!")
          case other => println("Unknown data structure :" + other)
        }

      }

    } catch {
      case e: Exception =>
        VALogger.exception(e)
        null
    }

  }

}
