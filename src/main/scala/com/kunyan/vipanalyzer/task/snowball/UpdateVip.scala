package com.kunyan.vipanalyzer.task.snowball

import java.sql.DriverManager

import org.apache.log4j.{Level, LogManager}

import scala.io.Source
import scala.xml.XML

/**
  * Created by yang on 5/20/16.
  */
object UpdateVip extends App {

  val configFile = XML.loadFile(args(0))

  Class.forName("com.mysql.jdbc.Driver")
  val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

  sys.addShutdownHook {
    connection.close()
  }

  LogManager.getRootLogger.setLevel(Level.WARN)

  val sql = s"select user_id from vip_snowball"
  val statement = connection.createStatement()
  val result = statement.executeQuery(sql)
  val updateStatement = connection.prepareStatement("update vip_snowball set official_vip=? where user_id=?")

  while (result.next()) {

    val userId = result.getString("user_id")
    val url = s"http://moer.jiemian.com/wapcommon_findMyFansCount.json?userId=$userId"
    val json = Source.fromURL(url).mkString
    val count = json.split("\"count\":")(1).split("}")(0).trim().toInt

    updateStatement.setInt(1, count)
    updateStatement.setString(2, userId)
    updateStatement.executeUpdate()
  }

  updateStatement.close()
  statement.close()
  connection.close()

}
