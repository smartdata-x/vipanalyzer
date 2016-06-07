package com.kunyan.vipanalyzer.parser.streaming

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/6/6.
  * 摩尔金融
  */
object Moer {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(html: String) = {

    val doc = Jsoup.parse(html, "UTF-8")
    val list = doc.getElementsByAttributeValue("class", "blu authortab-list").select("tr")

    for (i <- 0 until list.size) {

      val title = list.get(i).select("a").get(0).text()
      val date = list.get(i).select("div span").get(0).text()
      val content = list.get(i).select("p").text()

      (title, date, content)
    }

  }

}
