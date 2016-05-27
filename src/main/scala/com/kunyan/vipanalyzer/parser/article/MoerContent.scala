package com.kunyan.vipanalyzer.parser.article

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/5/27.
  */
object MoerContent {

  def getContent(html: String) = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      var title = doc.select("head").select("title").text

      if (title.contains("-摩尔金融")) {
        title = title.replace("-摩尔金融", "")
      }

      var content = doc.getElementsByAttributeValue("class", "left-content").select("p").text().trim()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}
