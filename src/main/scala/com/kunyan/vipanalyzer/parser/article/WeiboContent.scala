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
    * @return 提取的正文 标题
    */
  def parse(html: String): (String, String) = {

    val doc = Jsoup.parse(html, "UTF-8")

    try {

      var title = doc.select("head").select("title").text

      if (title.contains("- 微博")) {
        title = title.replace("- 微博", "")
      }

      val index = doc.getElementsByTag("script").size - 6
      val docStr = doc.getElementsByTag("script").get(index).toString
      val result = docStr.substring(16, docStr.length - 10)
      val jsonInfo = JSON.parseFull(result)
      var value = ""

      if (jsonInfo.isEmpty) {

        println("\"JSON parse value is empty,please have a check!\"")

      } else {
        jsonInfo match {

          case Some(mapInfo: Map[String, AnyVal]) => {

            val newHtml = mapInfo.get("html").get.toString
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
        e.printStackTrace()
        null
    }

  }

}
