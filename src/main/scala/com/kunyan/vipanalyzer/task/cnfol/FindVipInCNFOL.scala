package com.kunyan.vipanalyzer.task.cnfol

import java.sql.DriverManager

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser.CNFOLParser
import com.kunyan.vipanalyzer.util.{DBUtil, StringUtil}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON
import scala.xml.{Elem, XML}

/**
  * Created by yang on 5/15/16.
  * 在中金博客粉丝数少于 MAX_FOLLOWERS_COUNT 的用户中寻找官方认证的大
  */
object FindVipInCNFOL {

  val MAX_FOLLOWERS_COUNT = 500

  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile(args(0))

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("VIP ANALYZER")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lazyConn = LazyConnections(configFile)
    val connectionsBr = ssc.sparkContext.broadcast(lazyConn)

    val groupId = (configFile \ "kafka" \ "vip").text
    val brokerList = (configFile \ "kafka" \ "brokerList").text
    val receiveTopic = (configFile \ "kafka" \ "receive").text
    val sendTopic = (configFile \ "kafka" \ "send").text
    val topicsSet = Set[String](receiveTopic)

    sendHomePage(configFile, lazyConn, sendTopic)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> groupId)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {

      rdd.foreach(message => {
        analyzer(message, connectionsBr.value, sendTopic)
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 发送爬取粉丝数量少于500的用户的请求
    *
    * @param configFile 配置文件
    * @param lazyConn   连接容器
    * @param sendTopic  队列名
    */
  def sendHomePage(configFile: Elem, lazyConn: LazyConnections, sendTopic: String): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    sys.addShutdownHook {
      connection.close()
    }

    LogManager.getRootLogger.setLevel(Level.WARN)

    val sql = s"select user_id from user_cnfol where followers_count < $MAX_FOLLOWERS_COUNT group by user_id order by followers_count desc;"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    while (result.next()) {
      val userId = result.getString("user_id")
      lazyConn.sendTask(sendTopic, StringUtil.getUrlJsonString(Platform.CNFOL.id, s"http://blog.cnfol.com/$userId", 0))
    }

    statement.close()
    connection.close()
  }

  def analyzer(message: String, lazyConn: LazyConnections, topic: String): Unit = {

    VALogger.warn(message.replaceAll("\\n", ""))

    val json: Option[Any] = JSON.parseFull(message)

    if (json.isDefined) {

      //get content from json
      val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

      try {

        val attrId = map.get("attr_id").get.toInt
        val tableName = map.get("key_name").get
        val rowkey = map.get("pos_name").get

        val result = DBUtil.query(tableName, rowkey, lazyConn)

        attrId match {
          case id if id == Platform.CNFOL.id =>
            CNFOLParser.checkOfficialVip(result._1, result._2, lazyConn, topic)
        }

      } catch {

        case e: NoSuchElementException =>
          VALogger.error("json格式不正确" + json)

      }

    } else {

      VALogger.warn("this source isn't standard json" + message)

    }

  }

}
