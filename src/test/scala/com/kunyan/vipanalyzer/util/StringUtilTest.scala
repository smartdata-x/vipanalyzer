package com.kunyan.vipanalyzer.util

import org.scalatest.{Matchers, FlatSpec}
/**
  * Created by niujiaojiao on 2016/7/14.
  */
class StringUtilTest  extends FlatSpec with Matchers {

  it should "" in {

    val res = StringUtil.getUrlJsonString(5,"http://www/baidu.com",3)
    val str = "{\"id\":\"\", \"attrid\":\"5\", \"cookie\":\"\", \"referer\":\"\", \"url\":\"http://www/baidu.com\""
    val lastStr = "\"result\":\"3\"}"
    assert(res.contains(str))
    assert(res.contains(lastStr))

  }

  it should " toJson result" in {

    val res = StringUtil.toJson("5",3,"http://www/baidu.com")
    val str = "\"islogin\":3,\"attrid\":5, \"depth\":2, \"cur_depth\":2, \"method\":2, \"url\":\"http://www/baidu.com\"}"
    assert(res.contains(str))

  }

}
