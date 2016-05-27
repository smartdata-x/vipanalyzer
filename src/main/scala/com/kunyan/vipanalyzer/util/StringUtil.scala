package com.kunyan.vipanalyzer.util

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

}
