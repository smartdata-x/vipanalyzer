package com.kunyan.vipanalyzer.util

import java.sql.{PreparedStatement, Connection, CallableStatement, DriverManager}

import com.ibm.icu.text.CharsetDetector
import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser.SnowBallParser
import com.kunyandata.nlpsuit.summary.SummaryExtractor
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}

import scala.xml.{Elem, XML}

/**
  * Created by yang on 5/12/16.
  */
object DBUtil {

  def insertCNFOL(userId: String, followersCount: Int, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.preparedStatement

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

    val prep = lazyConn.preparedStatement

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

    val prep = lazyConn.preparedStatement

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

    val prep = lazyConn.preparedStatement

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

    val prep = lazyConn.preparedStatement

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

    val prep = lazyConn.preparedStatement

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

  def insertTGBArticle(userId: String, title: String, recommend: Boolean, read: Int, comment: Int, url: String, ts: Long, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.preparedStatement

    try {

      prep.setString(1, userId)
      prep.setString(2, title)
      if (recommend)
        prep.setInt(3, 1)
      else
        prep.setInt(3, 0)
      prep.setInt(4, read)
      prep.setInt(5, comment)
      prep.setString(6, url)
      prep.setLong(7, ts)
      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  /**
    * 雪球用户数据探索
    */
  def insertSnowBallUserInfo(url: String, userId: String, followersCount: Int, lazyConn: LazyConnections, topic: String): Unit = {

    val prep = lazyConn.preparedStatement

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

    var prep = lazyConn.preparedStatement

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

      if (url == null && content == null) {
        VALogger.error(s"Get empty data by this table: $tableName and rowkey: $rowkey")
        return null
      }

      val encoding = new CharsetDetector().setText(content).detect().getName

      (new String(url, "UTF-8"), new String(content, encoding))
    } catch {

      case e: Exception =>
        e.printStackTrace()
        null

    }

  }

  def queryContent(tableName: String, rowkey: String, lazyConn: LazyConnections): String = {

    val table = lazyConn.getTable(tableName)
    val get = new Get(rowkey.getBytes)

    try {

      val content = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      if (content == null) {
        VALogger.error(s"Get empty data by this table: $tableName and rowkey: $rowkey")
        return null
      }

      val encoding = new CharsetDetector().setText(content).detect().getName

      new String(content, encoding)
    } catch {

      case e: Exception =>
        e.printStackTrace()
        null

    }

  }

  def insert(lazyConn: LazyConnections, params: Any*): Unit = {

    val prep = lazyConn.preparedStatement

    try {

      for (i <- params.indices) {

        val param = params(i)

        param match {
          case param: String =>
            prep.setString(i + 1, param)
          case param: Int =>
            prep.setInt(i + 1, param)
          case param: Boolean =>
            prep.setBoolean(i + 1, param)
          case param: Long =>
            prep.setLong(i + 1, param)
          case param: Double =>
            prep.setDouble(i + 1, param)
          case _ =>
            VALogger.error("Unknown Type")

        }
      }

      prep.executeUpdate

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

    }

  }

  def insertCall(call: CallableStatement, params: Any*): Boolean = {

    try {

      for (i <- params.indices) {

        val param = params(i)

        param match {
          case param: String =>
            call.setString(i + 1, param)
          case param: Int =>
            call.setInt(i + 1, param)
          case param: Boolean =>
            call.setBoolean(i + 1, param)
          case param: Long =>
            call.setLong(i + 1, param)
          case param: Double =>
            call.setDouble(i + 1, param)
          case param: Short =>
            call.setShort(i + 1, param)
          case _ =>
            VALogger.error("Unknown Type")
        }
      }

      call.executeUpdate

      true

    } catch {

      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)

        false
    }

  }

  /**
    * 保存雪球文章信息
    */
  def insertSnowBallArticle(userId: String, title: String, retweet: Int, reply: Int, url: String, ts: Int, lazyConn: LazyConnections): Unit = {

    var prep = lazyConn.preparedStatement

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

  /**
    * 保存雪球 vip 信息
    */
  def insertWeiboVipInfo(userId: String, followersCount: Int, vip: Boolean, name: String, introduction: String, homePage: String, portrait: String, lazyConn: LazyConnections): Unit = {

    var prep = lazyConn.preparedStatement

    if (prep.isClosed) {

      Class.forName("com.mysql.jdbc.Driver")
      val configFile = XML.loadFile("/Users/yang/code/SmartData/vipanalyzer/src/main/resources/config.xml")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
      prep = connection.prepareStatement("INSERT INTO vip_weibo (user_id, followers_count, official_vip, name, introduction, home_page, portrait) VALUES (?,?,?,?,?,?,?)")

    }

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.setBoolean(3, vip)
      prep.setString(4, name)
      prep.setString(5, introduction)
      prep.setString(6, homePage)
      prep.setString(7, portrait)
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

  /**
    * 获取摘要
    *
    * @author sijiansheng
    * @param content 正文内容
    * @return 返回的300字摘要
    */
  def interceptData(content: String, number: Int): String = {

    var summary: String = ""

    if (content.length > number) {
      summary = content.substring(0, number)
    } else {
      summary = content
    }

    summary
  }

  /**
    * 获取完成字符(获取最后一个标识符前的数据)
    *
    * @param data 原数据
    * @param sign 标志数据
    * @return
    */
  def getLastSignData(data: String, sign: String): String = {

    if (data.contains(sign))
      data.substring(0, data.lastIndexOf(sign))
    else data

  }

  /**
    * 获取完成字符(获取第一个标识符前的数据)
    *
    * @param data 原数据
    * @param sign 标志数据
    * @return
    */
  def getFirstSignData(data: String, sign: String): String = {

    if (data.contains(sign))
      data.substring(0, data.indexOf(sign))
    else data

  }

  def getDigest(url: String, content: String, extractSummaryConfiguration: (String, Int)): String = {

    try {

      SummaryExtractor.extractSummary(content, extractSummaryConfiguration._1, extractSummaryConfiguration._2)
    } catch {

      case e: Exception =>
        VALogger.error(s"获取digest错误，url是$url")
        VALogger.exception(e)
        null

    }

  }


  def getNewStock(lastStock: String, stockDict: scala.collection.Map[scala.Predef.String, scala.Array[scala.Predef.String]]): String = {

    lastStock.split(",").map(cate => {
      cate + "=" + stockDict(cate).filterNot(_ == cate)(0)
    }).mkString("&")

  }


  def insertNewsToMysql(prep: PreparedStatement, params: Any*): Boolean = {

    try {

      for (i <- params.indices) {

        val param = params(i)

        param match {

          case param: String =>
            prep.setString(i + 1, param)
          case param: Int =>
            prep.setInt(i + 1, param)
          case param: Boolean =>
            prep.setBoolean(i + 1, param)
          case param: Long =>
            prep.setLong(i + 1, param)
          case param: Double =>
            prep.setDouble(i + 1, param)
          case _ =>
            VALogger.error("Unknown Type")

        }
      }
      prep.executeUpdate

      true
    } catch {

      case e: Exception =>
        VALogger.exception(e)
        false
    }

  }


}
