package com.kunyan.vipanalyzer.parser.streaming

import java.text.SimpleDateFormat

import org.jsoup.Jsoup
import org.scalatest.{Matchers, FlatSpec}

import scala.io.Source
import scala.util.control.Breaks._
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/12.
  */
class WeiboStreamingParserTest extends FlatSpec with Matchers {

  it should "parse platform weibo correctly" in {

    val html = Source.fromFile("/vipanalyzer/src/main/resources/test_html/weibo.html").mkString

    assert(html.nonEmpty)

    val doc = Jsoup.parse(html, "UTF-8")

    var index = 0

    for (i <- 0 until doc.getElementsByTag("script").size()) {

      if (doc.getElementsByTag("script").get(i).toString.contains("pl.content.homefeed.index")) {
        index = i
      }

    }

    val docStr = doc.getElementsByTag("script").get(index).toString
    val result = docStr.substring(16, docStr.length - 10)
    val jsonInfo = JSON.parseFull(result)

    if (jsonInfo.isEmpty) {

      assert(jsonInfo.nonEmpty)

    } else {

      jsonInfo match {

        case Some(mapInfo) =>

          val newHtml = mapInfo.asInstanceOf[Map[String, AnyVal]].get("html").get.toString

          val newDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_cardwrap WB_feed_type S_bg2")

          val anotherDoc = Jsoup.parse(newHtml, "UTF-8").getElementsByAttributeValue("class", "WB_cardwrap WB_feed_type S_bg2 WB_feed_vipcover")

          var div = anotherDoc.get(0)

          breakable {

            for (i <- 0 until newDoc.size + anotherDoc.size()) {

              if (i < anotherDoc.size) {

                div = anotherDoc.get(i)

              } else if (i >= anotherDoc.size) {

                div = newDoc.get(i - anotherDoc.size)

              }

              val children = div.getElementsByAttributeValue("class", "WB_detail").get(0)

              val expand = children.select("div.WB_feed_expand")

              if (expand.isEmpty) {

                val date = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("title")

                val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")

                val timeStamp = fm.parse(date).getTime

                val url = children.getElementsByAttributeValue("class", "WB_from S_txt2").get(0).select("a").get(0).attr("href")

                if (!url.contains("?ref=home&rid=")) {
                  break()
                }

                val getUrl = url.split("ref=home")(0)

                assert(getUrl.nonEmpty)

                val totalUrl = "http://weibo.com" + getUrl + "type=comment"

                var result = "" //用户发的文章标题

                if (children.toString.contains("WB_media_wrap clearfix")) {

                  var title = children.getElementsByAttributeValue("class", "WB_media_wrap clearfix").get(0)

                  if (title != null && title.toString.contains("WB_feed_spec_info SW_fun")) {

                    title = title.getElementsByAttributeValue("class", "WB_feed_spec_info SW_fun").get(0)

                    if (title != null && title.toString.contains("WB_feed_spec_cont")) {
                      result = title.getElementsByAttributeValue("class", "WB_feed_spec_cont").select("h4").text()
                    }

                  }

                }

                var user = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").text()

                val userCard = children.getElementsByAttributeValue("class", "W_f14 W_fb S_txt1").attr("usercard")

                val userId = userCard.split("id=")(1).split("&")(0)

                val botChild = div.getElementsByAttributeValue("class", "WB_feed_handle")

                val forward = botChild.select("li span.pos em")

                var forwardContent = ""

                if (forward.size >= 4) {
                  forwardContent = forward.get(3).text()
                } else {
                  forwardContent = 0.toString
                }

                if (forwardContent.startsWith("转发")) {
                  forwardContent = 0.toString
                }

                val repeat = botChild.select("li span.pos em")
                var repeatContent = ""

                if (repeat.size >= 6) {
                  repeatContent = repeat.get(5).text()
                } else {
                  repeatContent = 0.toString
                }

                if (repeatContent.startsWith("评论")) {
                  repeatContent = 0.toString
                }

                val like = botChild.select("li span.pos em")
                var likeContent = ""

                if (like.size >= 7) {
                  likeContent = like.get(6).text()
                } else {
                  likeContent = 0.toString
                }

                if (likeContent.startsWith("赞")) {
                  likeContent = 0.toString
                }

                if (user.isEmpty) {
                  user = children.getElementsByAttributeValue("class", "W_fb S_txt1").text()
                }

                if (result.isEmpty) {
                  result = user + "的观点："
                }

                assert(userId.length == 10)

                assert(result.nonEmpty)

                assert(forwardContent.toInt >= 0)

                assert(repeatContent.toInt >= 0)

                assert(likeContent.toInt >= 0)

              }

            }

          }

        case None => println("content is null!")

      }

    }

  }

}

