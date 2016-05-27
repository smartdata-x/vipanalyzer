package com.kunyan.vipanalyzer.parser.article

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/5/27.
  */
object TaoGuBaContent {

  def getContent(html: String) = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      var title = doc.select("head").select("title").text

      if (title.contains("_淘股吧")) {
        title = title.replace("_淘股吧", "")
      }

      var content = doc.getElementsByAttributeValue("class", "p_coten").text()
      val imgStr = doc.getElementsByAttributeValue("class", "p_wenz").get(0).select("img")

      if (imgStr != null) {
        //有的可能没有图片

        for (i <- 0 until imgStr.size) {

          var pictureUrl = imgStr.get(i).attr("src")

          if (!pictureUrl.startsWith("http://")) {
            pictureUrl = imgStr.get(i).attr("data-original")
          }

          if (!pictureUrl.startsWith("http://css.taoguba")) {
            val img = pictureUrl
          }

        }
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}
