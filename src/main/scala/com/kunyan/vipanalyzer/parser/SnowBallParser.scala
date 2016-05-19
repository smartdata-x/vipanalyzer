package com.kunyan.vipanalyzer.parser

import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}

import scala.util.parsing.json.JSON

/**
  * Created by yang on 5/10/16.
  * 雪球大V 解析类
  */
object SnowBallParser {

  val URL_PREFIX = "http://xueqiu.com/friendships/groups/members.json?page=1&count=2000&gid=0&uid="

  def parse(url: String, html: String, lazyConn: LazyConnections, topic: String): Unit = {

    if (url.contains("xueqiu.com/friendships/groups/members.json")) {

      if (!Scheduler.urlSet.contains(url)) {

        saveUserInfos(url, html, lazyConn, topic)
        val message = StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, url, 1)
        lazyConn.sendTask(topic, message)
        Scheduler.urlSet.add(url)

      }

    } else {

      VALogger.error(s"Invalid url: $url")

    }

  }

  /**
    * 提取粉丝数和用户 id 存入数据库
    * 向消息队列发送需要继续爬取的大V主页
    *
    * @param jsonString json 格式的消息字符串
    * @param lazyConn   连接容器
    */
  def saveUserInfos(url: String, jsonString: String, lazyConn: LazyConnections, topic: String): Unit = {

    Scheduler.urlSet.add(url)

    val json = JSON.parseFull(jsonString)
    val map = json.get.asInstanceOf[Map[String, String]]
    val userList = map.get("users").get.asInstanceOf[List[Map[String, Any]]]

    userList.foreach(x => {

      val id = x.get("id").get.asInstanceOf[Double].toLong.toString
      val followersNumber = x.get("followers_count").get.asInstanceOf[Double].toInt
      val url = URL_PREFIX + id

      if (!Scheduler.urlSet.contains(url) && followersNumber > 100) {

        DBUtil.insertSnowBallUserInfo(url, id, followersNumber, lazyConn, topic)

        val result = DBUtil.query(Platform.SNOW_BALL.id.toString, url, lazyConn)

        if (result == null || result._1.isEmpty || result._2.isEmpty) {

          lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, url, 2))

        } else {

          saveUserInfos(result._1, result._2, lazyConn, topic)

        }

      }

    })

  }

  def sendFirstPatch(lazyConn: LazyConnections, topic: String): Unit = {

    val url = URL_PREFIX + "4832309147"
    val result = DBUtil.query(Platform.SNOW_BALL.id.toString, url, lazyConn)

    if (result == null || result._1.isEmpty || result._2.isEmpty) {

      lazyConn.sendTask(topic, StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, url, 2))

    } else {

      saveUserInfos(result._1, result._2, lazyConn, topic)
      val message = StringUtil.getUrlJsonString(Platform.SNOW_BALL.id, url, 1)
      lazyConn.sendTask(topic, message)
      Scheduler.urlSet.add(url)

    }

  }

}
