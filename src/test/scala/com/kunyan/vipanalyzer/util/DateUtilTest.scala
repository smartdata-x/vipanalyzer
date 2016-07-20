package com.kunyan.vipanalyzer.util

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by niujiaojiao on 2016/7/14.
  */
class DateUtilTest extends FlatSpec with Matchers {

  it should "getDateString  is  long" in {

    val str = DateUtil.getDateString(1468463935*1000.toLong)
    val date = "20160714103855"
    str should be (date)

  }

  it should " " in {

    val str = DateUtil.getTimeStamp("2016-07-14:10-38-55","yyyy-MM-dd:HH-mm-ss")
    val date = "1468463935000".toLong
    str should be (date)

  }

}
