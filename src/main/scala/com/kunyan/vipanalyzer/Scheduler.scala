package com.kunyan.vipanalyzer

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser.SnowBallParser
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON
import scala.xml.XML

/**
  * Created by yang on 5/10/16.
  */
object Scheduler {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("VIP ANALYZER")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val path = args(0)

    val configFile = XML.loadFile(path)
    val connectionsBr = ssc.sparkContext.broadcast(LazyConnections(configFile))

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

    messages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {

      rdd.foreach(message => {
        analyzer(message, connectionsBr.value, sendTopic)
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

  def analyzer(message: String, lazyConn: LazyConnections, topic: String): Unit = {

    println(message)

    val json: Option[Any] = JSON.parseFull(message)

    if (json.isDefined) {

      //get content from json
      val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

      try {

        val attrId = map.get("attr_id").get.toInt
        val tableName = map.get("key_name").get
        val rowkey = map.get("pos_name").get

        val result = query(tableName, rowkey, lazyConn)

        if (attrId == Platform.Snowball.id) {
          SnowBallParser.parse(result, lazyConn, topic)
        }

      } catch {
        case e: NoSuchElementException =>
          VALogger.error("json格式不正确" + json)
      }

    } else {
      VALogger.warn("this source isn't standard json" + message)
    }

  }

  /**
    * 根据表名和rowkey从hbase中获取数据
    *
    * @param tableName 表名
    * @param rowkey 索引
    * @param lazyConn 连接容器
    * @return (url, html)
    */
  def query(tableName: String, rowkey: String, lazyConn: LazyConnections): String = {

    val table = lazyConn.getTable(tableName)
    val get = new Get(rowkey.getBytes)

    try {

      val result = table.get(get)

      new String(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content")))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }

  }

}
