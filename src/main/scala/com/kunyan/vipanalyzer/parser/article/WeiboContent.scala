package com.kunyan.vipanalyzer.parser.article

import org.jsoup.Jsoup
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/5/27.
  * 微博提取正文
  */
object WeiboContent {

  /**
    * 微博提取正文解析
    *
    * @param html 将要解析的字符串
    * @return 标题，正文
    */
  def getContent(html: String): (String, String) = {

    val doc = Jsoup.parse(html, "UTF-8")

    try {

      var title = doc.select("head").select("title").text

      if (title.contains("- 微博")) {
        title = title.replace("- 微博", "")
      }

      var index = 0

      for (i <- 0 until doc.getElementsByTag("script").size()) {

        if (doc.getElementsByTag("script").get(i).toString.contains("pl.content.weiboDetail.index"))
          index = i

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
            val newDoc = Jsoup.parse(newHtml, "UTF-8")
            val children = newDoc.getElementsByAttribute("tbinfo").get(0)
            val textComment = children.getElementsByAttributeValue("node-type", "feed_list_content").text()
            val textOrigin = children.getElementsByAttributeValue("node-type", "feed_list_reason").text()
            value = textComment + " " + textOrigin
          }

          case None => println("Parsing failed!")
          case other => println("Unknown data structure :" + other)
        }

      }

      (title, value)
    } catch {
      case e: Exception =>
        VALogger.exception(e)
        null
    }

  }

}
