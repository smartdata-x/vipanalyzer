package com.kunyan.vipanalyzer.parser

import com.kunyan.vipanalyzer.logger.VALogger

import scala.util.parsing.json.JSON

/**
  * Created by yang on 5/10/16.
  * 雪球解析类
  */
object SnowBallParser {

  /**
    * 雪球解析类入口
    *
    * @param jsonString 将要解析的页面的信息字符串
    * @return 返回用户ID和粉丝的数量的集合
    */
  def parseJson(jsonString: String): Map[String, String] = {

    val json = JSON.parseFull(jsonString)
    var map = Map[String, String]()

    if (json.isEmpty) {

      VALogger.error("\"JSON parse value is empty,please have a check!\"")
    } else {

      json match {
        case Some(mapInfo: Map[String, List[Map[String, AnyVal]]]) => map = getInfo(mapInfo)
        case None => VALogger.error("Parsing failed!")
        case other => VALogger.error("Unknown data structure :" + other)
      }

    }

    map

  }

  /**
    * 解析JSON字符串
    *
    * @param map 将要解析的map字符串集合
    * @return 返回用户ID和粉丝数量的集合
    */
  def getInfo(map: Map[String, List[Map[String, AnyVal]]]): Map[String, String] = {

    var result = Map[String, String]()

    try {

      val value = map.get("users").get

      for (i <- value.indices) {

        val idText = value(i).get("id").get.toString //此处的id需要进行特殊处理 3.405796236E9 => 3405796236
        var id = ""
        var num = ""

        if (idText.contains(".")) {
          val text = idText.replace(".", "")
          id = text.substring(0, text.length - 2)
        }

        val numText = value(i).get("followers_count").get.toString

        if (numText.contains(".0")) {
          num = numText.substring(0, numText.length - 2)
        }

        result = result + (id -> num)
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        null

    }

    result

  }

}
