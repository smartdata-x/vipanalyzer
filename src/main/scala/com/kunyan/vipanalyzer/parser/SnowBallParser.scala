package com.kunyan.vipanalyzer.parser

import java.util.Date

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.DBUtil

import scala.util.parsing.json.JSON

/**
  * Created by yang on 5/10/16.
  * 雪球大V 解析类
  */
object SnowBallParser {

  /**
    * 提取粉丝数和用户 id 存入数据库
    * 向消息队列发送需要继续爬取的大V主页
    *
    * @param jsonString json 格式的消息字符串
    * @param lazyConn   连接容器
    */
  def parse(jsonString: String, lazyConn: LazyConnections, topic: String): Unit = {

    val json = JSON.parseFull(jsonString)
    val map = json.get.asInstanceOf[Map[String, String]]
    val userList = map.get("users").get.asInstanceOf[List[Map[String, Any]]]

    userList.foreach(x => {

      val id = x.get("id").get.asInstanceOf[Double].toLong.toString
      val followersNumber = x.get("followers_count").get.asInstanceOf[Double].toInt

      val url = s"https://xueqiu.com/friendships/groups/members.json?page=1&count=2000&uid=$id&gid=0"
      lazyConn.sendTask(topic, getUrlJsonString(url))

      DBUtil.insertCNFOL(id, followersNumber, lazyConn)
    })

  }

  /**
    * 拼接往algo_vip topic 发的消息的json字符串
    *
    * @param url 消息中的帖子的url
    * @return json格式的消息的字符串
    */
  def getUrlJsonString(url: String): String = {
    val json = "{\"id\":\"\", \"attrid\":\"%d\", \"cookie\":\"\", \"referer\":\"\", \"url\":\"%s\", \"timestamp\":\"%s\"}"
    json.format(Platform.SNOW_BALL.id, url, new Date().getTime.toString)
  }

}
