package com.kunyan.vipanalyzer.parser.streaming

import com.kunyan.vipanalyzer.logger.VALogger
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/6/6.
  * 淘股吧
  */
object Taoguba {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(html: String) = {

    val jsonInfo = JSON.parseFull(html)

    try {

      if (jsonInfo.isEmpty) {

        println("\"JSON parse value is empty,please have a check!\"")
      } else {

        jsonInfo match {

          case Some(mapInfo) =>

            val recordValue = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("record", "").asInstanceOf[List[Map[String, AnyVal]]]

            for (i <- recordValue.indices) {

              val value = recordValue(i).asInstanceOf[Map[String, String]]
              val date = value.getOrElse("actionDate", "")
              val userID = value.getOrElse("userID", "")
              val userName = value.getOrElse("userName", "")
              val body = value.getOrElse("body", "")

            }

          case None => println("Parsing failed!")
          case other => println("Unknown data structure :" + other)

        }
      }
    } catch {
      case e: Exception =>
        VALogger.exception(e)
        null
    }
  }

}
