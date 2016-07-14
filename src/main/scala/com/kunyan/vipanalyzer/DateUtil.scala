package com.kunyan.vipanalyzer

import java.text.SimpleDateFormat

/**
  * Created by yang on 5/13/16.
  */
object DateUtil {

  def getDateString: String = getDateString(System.currentTimeMillis())

  def getDateString(timestamp: Long): String = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)

  def getTimeStamp(dateStr: String, formatStr: String): Long = {

    val sdf = new SimpleDateFormat(formatStr)
    sdf.parse(dateStr).getTime

  }

}
