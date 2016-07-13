package com.kunyan.vipanalyzer.parser.streaming

import org.scalatest.{Matchers, FlatSpec}

import scala.io.Source
import scala.util.control.Breaks._
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/12.
  */
class TaogubaStreamingParserTest extends FlatSpec with Matchers {

  it should "parse platform Taoguba correctly" in {

    val html = Source.fromFile("/vipanalyzer/src/main/resources/test_html/taoguba.txt").mkString

    val jsonInfo = JSON.parseFull(html)

    if (jsonInfo.isEmpty) {

      assert(jsonInfo.nonEmpty)

    } else {

      jsonInfo match {

        case Some(mapInfo) =>

          val recordValue = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("record", "").asInstanceOf[List[Map[String, AnyVal]]]

          breakable {

            for (i <- recordValue.indices) {

              val value = recordValue(i).asInstanceOf[Map[String, String]]
              val date = value.getOrElse("actionDate", "")

              val userID = value.getOrElse("userID", "")
              val objectID = value.getOrElse("objectID", "")
              val otherID = value.getOrElse("OtherID", "")
              val userName = value.getOrElse("userName", "")
              val title = userName + "的观点: "
              var content = value.getOrElse("body", "")

              if (content.contains("[quote]")) {

                val text = content.split("[quote]")

                if (text.nonEmpty) {
                  content = text(0)
                }

              }

              //过滤掉此类ID
              if (otherID.toInt != 0) {

                val url = "http://www.taoguba.com.cn/Article" + "/" + objectID + "/" + otherID

//                println(s"$userID+$objectID+$otherID+$userName+$title+$content")
                assert(userID.nonEmpty)

                assert(objectID.nonEmpty)

                assert(otherID.nonEmpty)

                assert(userName.nonEmpty)

                assert(title.nonEmpty)

                assert(content.nonEmpty)

              }

            }

          }

      }

    }

  }

}

