package com.kunyan.vipanalyzer.parser.streaming

import org.jsoup.Jsoup
import org.scalatest.{Matchers, FlatSpec}
import scala.util.parsing.json.JSON
import scala.io.Source

/**
  * Created by niujiaojiao on 2016/7/12.
  */
class SnowballSteamingParserTest extends FlatSpec with Matchers {

  it should "parse platform snowball correctly" in {

    val html = Source.fromFile("/vipanalyzer/src/main/resources/test_html/snowball.html").mkString

    assert(html.nonEmpty)

    val doc = Jsoup.parse(html, "UTF-8")

    var title = ""
    val user = doc.select("title").text()

    if (user.contains("- 雪球")) {
      if (user.split("- 雪球").size >= 1) {
        title = user.split("- 雪球")(0) + "的观点："
      }
    }

    assert(title.nonEmpty)

    var index = 0

    for (i <- 0 until doc.getElementsByTag("script").size()) {

      if (doc.getElementsByTag("script").get(i).toString.contains("SNB.data.statuses =")) {
        index = i
      }

    }

    val contentStr = doc.getElementsByTag("script").get(index).toString.split("SNB.data.statusType =")(0).split("SNB.data.statuses =")(1).trim

    val resultContent = contentStr.substring(0, contentStr.length - 1)

    val jsonInfo = JSON.parseFull(resultContent)

    if (jsonInfo.isEmpty) {

      assert(jsonInfo.nonEmpty)

    } else {

      jsonInfo match {

        case Some(mapInfo) =>

          val content = mapInfo.asInstanceOf[Map[String, List[Map[String, Any]]]].getOrElse("statuses", "").asInstanceOf[List[Map[String, Any]]]

          for (i <- content.indices) {

            val mapInfo = content(i)

            val url = "https://xueqiu.com" + mapInfo.getOrElse("target", "")

            assert(mapInfo.getOrElse("target", "").toString.nonEmpty)

            var latestFlag = ""

            var flag = mapInfo.getOrElse("title", "")

            if (flag == null) {

              val text = mapInfo.getOrElse("description", "").toString

              if (text.length > 30) {
                latestFlag = text.substring(0, 30)
              } else {
                latestFlag = text
              }

            } else {

              latestFlag = flag.toString.replaceAll("<[^>]*>", "")
            }

            val identifyRetweet = mapInfo.getOrElse("retweeted_status", "") //转发标志

            if (identifyRetweet == null) {

              val userID = mapInfo.getOrElse("user_id", "")
              println(userID)
              val retweet = mapInfo.getOrElse("retweet_count", "").asInstanceOf[Double].toInt
              val reply = mapInfo.getOrElse("reply_count", "").asInstanceOf[Double].toInt

              val screenName = mapInfo.getOrElse("user", "").asInstanceOf[Map[String, String]]
              var name = screenName.getOrElse("screen_name", "")

              if ("" == name) {
                name = title
              } else {
                name = name + "的观点："
              }

              if ("" == flag) {
                flag = name
              }

              assert(title.nonEmpty)
              assert(userID.toString.nonEmpty)
              assert(retweet >= 0)
              assert(reply >= 0)
              assert(name.nonEmpty)
              assert(url.nonEmpty)
              assert(flag.toString.nonEmpty)

            }

          }

        case None => println("content is null!")
      }

    }

  }

}
