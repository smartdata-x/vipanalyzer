package com.kunyan.vipanalyzer.parser.streaming

import org.jsoup.Jsoup
import org.scalatest.{Matchers, FlatSpec}

import scala.io.Source

/**
  * Created by niujiaojiao on 2016/7/12.
  */
class MoerStreamingParserTest extends FlatSpec with Matchers{

  it should "parse platform moer correctly" in {

    val html = Source.fromFile("/vipanalyzer/src/main/resources/test_html/moer.html").mkString

    val doc = Jsoup.parse(html, "UTF-8")

    val list = doc.getElementsByAttributeValue("class", "blu authortab-list").select("tr")

    for (i <- 0 until list.size) {

      val title = list.get(i).select("a").get(0).text()

      val url = "http://moer.jiemian.com/" + list.get(i).select("a").get(0).attr("href")

      assert(title.nonEmpty)

      assert(url.startsWith("http://moer.jiemian.com/articleDetails.htm?articleId="))

      println(s"$title + $url")
    }

  }
}
