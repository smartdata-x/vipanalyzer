package com.kunyan.vipanalyzer.util

import java.util.Date

import com.kunyan.vipanalyzer.logger.VALogger

/**
  * Created by yang on 5/12/16.
  */
object StringUtil {

  def getUrlJsonString(platform: Int, url: String, result: Int): String = {

    val json = "{\"id\":\"\", \"attrid\":\"%d\", \"cookie\":\"\", \"referer\":\"\", \"url\":\"%s\", \"timestamp\":\"%s\", \"result\":\"%d\"}"

    val message = json.format(platform, url, DateUtil.getDateString, result)
    VALogger.warn(message)
    message
  }

  def toJson(attrId: String, islogin: Int, url: String): String = {
    val json = "{\"id\":%s, \"islogin\":%d,\"attrid\":%s, \"depth\":%d, \"cur_depth\":%d, \"method\":%s, \"url\":\"%s\"}"
    json.format(new Date().getTime.toString, islogin, attrId, 2, 2, "2", url)
  }

}
