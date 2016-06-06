package com.kunyan.vipanalyzer.parser.streaming

import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/6/3.
  * 中金博客解析
  */
object Cnfol extends App {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(html: String) = {

    val doc = Jsoup.parse(html, "UTF-8")
    val list = doc.select("div#Tab1 div.TabItem ul li")

    for (i <- 0 until list.size) {

      val children = list.get(i)
      val user = children.select("div.UserBox a.Name").text()
      val title = children.select("div.UserBox a.Tit").text()
      val content = children.select("p.ContentP").text()

      (user, title, content)
    }

  }

}
