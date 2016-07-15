package com.kunyan.vipanalyzer.parser.streaming

import org.jsoup.Jsoup
import org.scalatest.{Matchers, FlatSpec}
import scala.io.Source

/**
  * Created by niujiaojiao on 2016/7/12.
  */
class CnfolStreamingParserTest extends FlatSpec with Matchers {

  it should "parse platform cnfol correctly" in {

    val html = Source.fromFile("/vipanalyzer/src/main/resources/test_html/cnfol.html").mkString

    assert(html.nonEmpty)

    val doc = Jsoup.parse(html, "UTF-8")

    val list = doc.select("div#Tab1 div.TabItem ul li")

    for (i <- 0 until list.size) {

      val child = list.get(i)

      val title = child.select("div.UserBox a.Tit").text()

      val userId = child.select("div.UserBox a.Name").attr("href").split("/").last

      val recommended = child.select("div.UserBox a.Tit i.TuiJian").size().toShort

      val reproduce = child.select("div.HandleBox span:nth-of-type(2)").text.split(" ").last.toInt

      val comment = child.select("div.HandleBox span:nth-of-type(3)").text.split(" ").last.toInt

      val url = child.select("div.UserBox a.Tit").attr("href")

      assert(title.nonEmpty)

      assert(userId.nonEmpty)

      assert(recommended >= 0)

      assert(reproduce >= 0)

      assert(comment >= 0)

      assert(url.startsWith("http://blog.cnfol.com"))

    }

  }

}
