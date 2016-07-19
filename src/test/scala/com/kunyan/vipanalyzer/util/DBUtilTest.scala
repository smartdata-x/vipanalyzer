package com.kunyan.vipanalyzer.util

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by niujiaojiao on 2016/7/14.
  */
class DBUtilTest extends FlatSpec with Matchers {

  it should "get Substring  string" in {

    val str = "中国南海问题"
    val substring = "中国南海"
    val res = DBUtil.interceptData(str,4)
    res  should  be (substring)

  }

}
