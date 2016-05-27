package com.kunyan.vipanalyzer.parser.article

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/5/27.
  */
object SnowBallContent {

  def getContent(html: String) = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      var title = doc.select("head").select("title").text

      if(title.contains("- 雪球")){
        title = title.replace("- 雪球","")
      }

      var content = doc.getElementsByAttributeValue("class","detail").text()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}
