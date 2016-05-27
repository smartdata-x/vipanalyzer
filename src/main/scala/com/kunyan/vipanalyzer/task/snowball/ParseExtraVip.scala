package com.kunyan.vipanalyzer.task.snowball

import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.parser.SnowBallParser
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil}
import org.apache.log4j.{Level, LogManager}

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.xml.XML

/**
  * Created by yang on 5/21/16.
  */
object ParseExtraVip {

  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile(args(0))

    LogManager.getRootLogger.setLevel(Level.WARN)

    val lazyConn = LazyConnections(configFile)

    userList(lazyConn)
  }

  def homePage(lazyConn: LazyConnections): Unit = {

    for (url <- Source.fromFile(this.getClass.getResource("/vip_list.txt").getPath).getLines()) {

      val userId = url.split("com/")(1)

      val result = DBUtil.query(Platform.SNOW_BALL.id.toString, url, lazyConn)

      if (result == null) {

        println("null:" + url)

      } else {

        val json = JSON.parseFull(result._2.split("SNB.profileUser =")(1).split("</script>")(0))
        val map = json.get.asInstanceOf[Map[String, Any]]
        val followersCount = map.get("followers_count").get.asInstanceOf[Double].toInt
        val name = map.get("screen_name").get.asInstanceOf[String]
        val introduction = map.get("description").get.asInstanceOf[String]
        val home_page = url
        val portrait = "http://xavatar.imedao.com/" + map.get("profile_image_url").get.asInstanceOf[String].replaceAll("!50x50.png", "")

        DBUtil.insertSnowBallVipInfo(userId, followersCount, name, introduction, home_page, portrait, lazyConn)
      }

    }

  }

  def userList(lazyConn: LazyConnections): Unit = {

    for (url <- Source.fromFile(this.getClass.getResource("/vip_list.txt").getPath).getLines()) {

      val userId = url.split("com/")(1)
      val userList = SnowBallParser.URL_PREFIX + userId

      val result = DBUtil.query(Platform.SNOW_BALL.id.toString, userList, lazyConn)

      if (result == null) {

        println("null:" + userList)

      } else {

        saveVipInfo(result._1, result._2, lazyConn)

        /*val json = JSON.parseFull(result._2.split("SNB.profileUser =")(1).split("</script>")(0))
        val map = json.get.asInstanceOf[Map[String, Any]]
        val followersCount = map.get("followers_count").get.asInstanceOf[Double].toInt
        val name = map.get("screen_name").get.asInstanceOf[String]
        val introduction = map.get("description").get.asInstanceOf[String]
        val home_page = url
        val portrait = "http://xavatar.imedao.com/" + map.get("profile_image_url").get.asInstanceOf[String].replaceAll("!50x50.png", "")

        DBUtil.insertSnowBallVipInfo(userId, followersCount, name, introduction, home_page, portrait, lazyConn)*/
      }

    }

  }

  def saveVipInfo(url: String, html: String, lazyConn: LazyConnections): Unit = {

    val json = JSON.parseFull(html)
    val map = json.get.asInstanceOf[Map[String, String]]
    val userList = map.get("users").get.asInstanceOf[List[Map[String, Any]]]

    userList.foreach(x => {

      val userId = x.get("id").get.asInstanceOf[Double].toLong.toString

      val followersNumber = x.get("followers_count").get.asInstanceOf[Double].toInt
      val name = x.get("screen_name").get.asInstanceOf[String]

      if (name.length > 20)
        println(name)

      var introduction: String = x.get("description").get.asInstanceOf[String]

      if (introduction == null)
        introduction = ""

      val homePage = SnowBallParser.HOME_PAGE + userId
      val portrait = SnowBallParser.IMAGE_PREFIX + x.get("profile_image_url").get.asInstanceOf[String].split(",")(0)

      DBUtil.insertSnowBallVipInfo(userId, followersNumber, name, introduction, homePage, portrait, lazyConn)

    })

  }

}
