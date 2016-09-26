package com.kunyan.vipanalyzer

import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.parser.streaming._
import com.kunyan.vipanalyzer.util.DBUtil
import com.kunyandata.nlpsuit.classification.Bayes
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.KunyanConf
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

    val sparkConf = new SparkConf()
      .setAppName("VIP ANALYZER")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    VALogger.warn("App VIP starts \n")

    val path = args(0)

    val configFile = XML.loadFile(path)
    val lazyConn = LazyConnections(configFile)
    val connectionsBr = ssc.sparkContext.broadcast(lazyConn)

    val groupId = (configFile \ "kafka" \ "vip").text
    val brokerList = (configFile \ "kafka" \ "brokerList").text
    val receiveTopic = (configFile \ "kafka" \ "receive").text
    val sendTopic = (configFile \ "kafka" \ "send").text
    val snowBallTopic = (configFile \ "kafka" \ "duration").text
    val topicsSet = Set[String](receiveTopic)

    val stopWordsPath = (configFile \ "segment" \ "stopWords").text
    val modelsPath = (configFile \ "segment" \ "classModelAddress").text
    val sentiPath = (configFile \ "segment" \ "sentimentModelAddress").text
    val keyWordDictPath = (configFile \ "segment" \ "keyWords").text

    val kyConf = new KunyanConf()

    kyConf.set((configFile \ "segment" \ "ip").text, (configFile \ "segment" \ "port").text.toInt)

    val stopWords = Bayes.getStopWords(stopWordsPath)
    val classModels = Bayes.initModels(modelsPath)
    val sentiModels = PredictWithNb.init(sentiPath)
    val keyWordDict = Bayes.initGrepDicts(keyWordDictPath)

    val stopWordsBr = ssc.sparkContext.broadcast(stopWords)
    val classModelsBr = ssc.sparkContext.broadcast(classModels)
    val sentiModelsBr = ssc.sparkContext.broadcast(sentiModels)
    val keyWordDictBr = ssc.sparkContext.broadcast(keyWordDict)

    val extractSummaryConfiguration = ((configFile \ "summaryConfiguration" \ "ip").text, (configFile \ "summaryConfiguration" \ "port").text.toInt)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> groupId)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    LogManager.getRootLogger.setLevel(Level.WARN)

    messages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {

      rdd.foreach(message => {

        analyzer(message, connectionsBr.value, sendTopic, snowBallTopic,
          extractSummaryConfiguration,
          stopWordsBr.value,
          classModelsBr.value,
          sentiModelsBr.value,
          keyWordDictBr.value,
          kyConf
        )

      }
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def analyzer(message: String, lazyConn: LazyConnections, topic: String, snowBallTopic: String,
               summaryExtraction: (String, Int),
               stopWords: Array[String],
               classModels: scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, java.io.Serializable]]],
               sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
               keyWordDict: scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
               kyConf: KunyanConf): Unit = {

    val json: Option[Any] = JSON.parseFull(message)

    if (json.isDefined) {

      //get content from json
      val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

      try {

        val attrId = map.get("attr_id").get.toInt
        val tableName = map.get("key_name").get
        val rowkey = map.get("pos_name").get
        val result = DBUtil.query(tableName, rowkey, lazyConn)

        if (null == result) {
          VALogger.error("Get empty data from hbase table! Message :  " + message)
          return
        }

        attrId match {

          case id if id == Platform.SNOW_BALL.id =>
            SnowballStreamingParser.parse(result._1, result._2, lazyConn, snowBallTopic)
          case id if id == Platform.CNFOL.id =>
            CnfolStreamingParser.parse(result._1, result._2, lazyConn, topic)
          case id if id == Platform.TAOGUBA.id =>
            TaogubaStreamingParser.parse(result._1, result._2, lazyConn, topic,
              stopWords, classModels, sentimentModels, keyWordDict, kyConf, summaryExtraction)
          case id if id == Platform.MOER.id =>
            MoerStreamingParser.parse(result._1, result._2, lazyConn, topic)
          case id if id == Platform.WEIBO.id =>
            WeiboStreamingParser.parse(result._1, result._2, lazyConn, topic)
          case _ =>
            VALogger.warn(attrId.toString)

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
