package com.kunyan.vipanalyzer.util

import java.text.SimpleDateFormat

/**
  * Created by yang on 5/13/16.
  */
object DateUtil {

  def getDateString: String = getDateString(System.currentTimeMillis())

  def getDateString(timestamp: Long): String = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)

}
