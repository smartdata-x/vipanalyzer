package com.kunyan.vipanalyzer

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser._
import com.kunyan.vipanalyzer.task.snowball.SendHomePage
import com.kunyan.vipanalyzer.task.weibo.SendUrl
import com.kunyan.vipanalyzer.util.DBUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.parsing.json.JSON
import scala.xml.XML

/**
  * Created by yang on 5/10/16.
  */
object Scheduler {

  var urlSet = mutable.Set[String]()

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]")
      .setAppName("VIP ANALYZER")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val path = args(0)

    val configFile = XML.loadFile(path)
    val connectionsBr = ssc.sparkContext.broadcast(LazyConnections(configFile))
    val lazyConn = LazyConnections(configFile)

    val groupId = (configFile \ "kafka" \ "vip").text
    val brokerList = (configFile \ "kafka" \ "brokerList").text
    val receiveTopic = (configFile \ "kafka" \ "receive").text
    val sendTopic = (configFile \ "kafka" \ "send").text
    val topicsSet = Set[String](receiveTopic)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> groupId)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    LogManager.getRootLogger.setLevel(Level.WARN)

//    DBUtil.initUrlSet(configFile)
//    SnowBallParser.extractVip(lazyConn, sendTopic)
    SendUrl.sendHomePages(lazyConn, sendTopic)

    messages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {

      rdd.foreach(message => {
        analyzer(message, connectionsBr.value, sendTopic)
      })

    })

    ssc.start()
    ssc.awaitTermination()
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

          case id if id == Platform.SNOW_BALL.id =>
            SnowBallParser.parse(result._1, result._2, lazyConn, topic)
          case id if id == Platform.CNFOL.id =>
            CNFOLParser.parseBlog(result._1, result._2, lazyConn, topic)
          case id if id == Platform.TAOGUBA.id =>
            TaoGuBaParser.parse(result._1, result._2, lazyConn, topic)
          case id if id == Platform.MOER.id =>
            MoerParser.parse(result._1, result._2, lazyConn, topic)
          case id if id == Platform.WEIBO.id =>
            WeiboParser.parse(result._1, result._2, lazyConn, topic)

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
