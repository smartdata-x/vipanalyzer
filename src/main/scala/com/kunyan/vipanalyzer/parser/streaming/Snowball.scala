package com.kunyan.vipanalyzer.parser.streaming

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/6/3.
  * 雪球
  */
object Snowball {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(html: String) = {

    val doc = Jsoup.parse(html, "UTF-8")
    val list = doc.select("div.mod div#status-list div.status-item")

    for (i <- 0 until list.size) {

      val children = list.get(i)
      val user = children.select("div.status-hd a").attr("data-screenname")
      val content = children.select("div.status-bd  div.status-content div.detail  div.summary").text()
      val time = children.select("div.status-ft div.status-meta  div.meta-info a.time").text()
      val title = children.select("div.status-bd div.status-content h4.status-title  a").text()

      (user, title, content, time)
    }

  }

}
