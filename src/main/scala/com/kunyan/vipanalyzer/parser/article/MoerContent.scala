package com.kunyan.vipanalyzer.parser.article

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/5/27.
  * 摩尔金融解析
  */
object MoerContent {

  /**
    * 摩尔金融解析
    *
    * @param html 将要解析的字符串
    * @return 获取的标题，正文
    */
  def getContent(html: String): (String, String) = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      var title = doc.select("head").select("title").text

      if (title.contains("-摩尔金融")) {
        title = title.replace("-摩尔金融", "")
      }

      val content = doc.getElementsByAttributeValue("class", "left-content").select("p").text().trim()

      (title, content)
    } catch {
      case e: Exception =>
        VALogger.exception(e)
        null
    }

  }

}
