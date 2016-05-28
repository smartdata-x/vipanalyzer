package com.kunyan.vipanalyzer.parser.article

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/5/27.
  * 中金博客正文解析
  */
object CNFOContent {

  def getContent(html: String) = {

    try{

      val doc = Jsoup.parse(html, "UTF-8")
      val title = doc.select("head").select("title").text.split("_")(0)
      var content = doc.getElementsByAttributeValue("class", "ArticleCont ArtLink").select("p").text()
      val regex = "\\s+"
      val result = content.replaceAll(regex, "").trim()
      val imgStr = doc.getElementsByAttributeValue("class", "ArticleCont ArtLink").get(0).select("img")

      if (imgStr != null) {
        //有的可能没有图片
        val pictureUrl = imgStr.attr("src")
      }

    } catch  {
      case e:Exception =>
        e.printStackTrace()
    }

  }


}
