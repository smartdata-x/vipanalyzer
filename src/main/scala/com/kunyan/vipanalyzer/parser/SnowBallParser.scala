package com.kunyan.vipanalyzer.parser

import scala.util.parsing.json.JSON

/**
  * Created by yang on 5/10/16.
  */
object SnowBallParser {

  def parseJson(jsonString: String): Map[String, String] = {

    val json = JSON.parseFull(jsonString)
    val map = json.get.asInstanceOf[Map[String, String]]

    map
  }

}
