package com.kunyan.vipanalyzer.util

import java.sql.DriverManager

import com.ibm.icu.text.CharsetDetector
import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser.SnowBallParser
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}

import scala.xml.{XML, Elem}

/**
  * Created by yang on 5/12/16.
  */
object DBUtil {

  def insertCNFOL(userId: String, followersCount: Int, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  def saveCNFOLVip(userId: String, vip: Boolean, name: String, introduction: String, url: String, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setBoolean(2, vip)
      prep.setString(3, name)
      prep.setString(4, introduction)
      prep.setString(5, url)
      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  def saveBlogInfo(userId: String, title: String, recommend: Boolean, time: String, reproduce: Int, comment: Int, url: String, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setString(2, title)
      prep.setBoolean(3, recommend)
      prep.setString(4, time)
      prep.setInt(5, reproduce)
      prep.setInt(6, comment)
      prep.setString(7, url)
      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  def saveMOERVip(userId: String, followersCount: Int, vip: Int, name: String, introduction: String, url: String, portrait: String, field: String, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.setInt(3, vip)
      prep.setString(4, name)
      prep.setString(5, introduction)
      prep.setString(6, url)
      prep.setString(7, portrait)
      prep.setString(8, field)

      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  def insertTGB(userId: String, data: (Int, Int, Int), lazyConn: LazyConnections): Boolean = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setInt(2, data._1)
      prep.setInt(3, data._2)
      prep.setInt(4, data._3)
      prep.executeUpdate

      true

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)
        false
    }

  }

  def saveTGBVip(userId: String, followersCount: Int, vip: Int, name: String, introduction: String, homePage: String, portrait: String, marrow: Int, recommend: Int, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.setInt(3, vip)
      prep.setString(4, name)
      prep.setString(5, introduction)
      prep.setString(6, homePage)
      prep.setString(7, portrait)
      prep.setInt(8, marrow)
      prep.setInt(9, recommend)

      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error(s"向MySQL插入数据失败: $userId")
        e.printStackTrace()
    }

  }

  /**
    * 雪球用户数据探索
    */
  def insertSnowBallUserInfo(url: String, userId: String, followersCount: Int, lazyConn: LazyConnections, topic: String): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error(s"向MySQL插入数据失败: $userId")
        e.printStackTrace()

    }
  }

  /**
    * 保存雪球 vip 信息
    */
  def insertSnowBallVipInfo(userId: String, followersCount: Int, name: String, introduction: String, url: String, portrait: String, lazyConn: LazyConnections): Unit = {

    var prep = lazyConn.mysqlConn

    if (prep.isClosed) {

      Class.forName("com.mysql.jdbc.Driver")
      val configFile = XML.loadFile("/Users/yang/code/SmartData/vipanalyzer/src/main/resources/config.xml")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
      prep = connection.prepareStatement("INSERT INTO vip_snowball (user_id, followers_count, name, introduction, home_page, portrait) VALUES (?,?,?,?,?,?)")

    }

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.setString(3, name)
      prep.setString(4, introduction)
      prep.setString(5, url)
      prep.setString(6, portrait)
      prep.executeUpdate

      Scheduler.urlSet.add(userId)

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  /**
    * 根据表名和rowkey从hbase中获取数据
    *
    * @param tableName 表名
    * @param rowkey    索引
    * @param lazyConn  连接容器
    * @return (url, html)
    */
  def query(tableName: String, rowkey: String, lazyConn: LazyConnections): (String, String) = {

    val table = lazyConn.getTable(tableName)
    val get = new Get(rowkey.getBytes)

    try {

      val url = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val content = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      if (url == null || content == null) {
        VALogger.error(s"Get empty data by this table: $tableName and rowkey: $rowkey")
        return null
      }

      val encoding = new CharsetDetector().setText(content).detect().getName

      (new String(url, "UTF-8"), new String(content, encoding))
    } catch {

      case e: Exception =>
//        e.printStackTrace()
        null

    }

  }

  /**
    * 保存雪球文章信息
    */
  def insertSnowBallArticle(userId: String, title: String, retweet: Int, reply: Int, url: String, ts: Int, lazyConn: LazyConnections): Unit = {

    var prep = lazyConn.mysqlConn

    if (prep.isClosed) {

      Class.forName("com.mysql.jdbc.Driver")
      val configFile = XML.loadFile("/Users/yang/code/SmartData/vipanalyzer/src/main/resources/config.xml")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
      prep = connection.prepareStatement("INSERT INTO article_snowball (user_id, title, retweet, reply, url, ts) VALUES (?,?,?,?,?,?)")

    }

    try {

      prep.setString(1, userId)
      prep.setString(2, title)
      prep.setInt(3, retweet)
      prep.setInt(4, reply)
      prep.setString(5, url)
      prep.setInt(6, ts)
      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  def initUrlSet(configFile: Elem): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = s"select user_id from user_snowball"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    while (result.next()) {
      val userId = result.getString("user_id")
      Scheduler.urlSet.add(SnowBallParser.URL_PREFIX + userId)
    }

    statement.close()
    connection.close()
  }

}
