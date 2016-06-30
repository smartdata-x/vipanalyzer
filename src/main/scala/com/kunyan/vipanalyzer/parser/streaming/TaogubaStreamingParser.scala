package com.kunyan.vipanalyzer.parser.streaming

import java.sql.{PreparedStatement, CallableStatement}
import java.text.SimpleDateFormat
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger
import com.kunyan.vipanalyzer.util.{DBUtil, RedisUtil}
import com.kunyandata.nlpsuit.classification.Bayes
import com.kunyandata.nlpsuit.sentiment.PredictWithNb
import com.kunyandata.nlpsuit.util.{TextPreprocessing, KunyanConf}

import scala.util.control.Breaks._
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/6/7.
  * 淘股吧
  */
object TaogubaStreamingParser {
  /**
    * 解析获取信息
    *
    * @param html 将要解析的字符串
    */
  def parse(pageUrl: String,
            html: String,
            lazyConn: LazyConnections,
            topic: String,
            stopWords: Array[String],
            classModels: scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, java.io.Serializable]]],
            sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
            keyWordDict: scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
            kyConf: KunyanConf) = {

    val jsonInfo = JSON.parseFull(html)

    val cstmtArticle = lazyConn.mysqlConn.prepareCall("{call proc_InsertTaogubaNewArticle(?,?,?,?,?,?,?)}")

    val initial = lazyConn.jedisHget(RedisUtil.REDIS_HASH_NAME, pageUrl)

    var lastValue = 0L

    if ("" == initial || null == initial) {
      lastValue = 0L
    } else {
      lastValue = initial.toLong
    }

    val sql = "{call proc_InsertDigestTaoguba(?,?,?,?)}"

    val cstmtDigest = lazyConn.mysqlConn.prepareCall(sql)

    val newsMysqlStatement = lazyConn.mysqlNewsConn.prepareStatement("INSERT INTO news_info (type, platform, title, url, news_time, industry, section, stock, digest, summary, sentiment, updated_time, source)" +
      " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")

    try {

      if (jsonInfo.isEmpty) {

        VALogger.error("\"JSON parse value is empty,please have a check!\"")

      } else {

        jsonInfo match {

          case Some(mapInfo) =>

            val recordValue = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("record", "").asInstanceOf[List[Map[String, AnyVal]]]

            breakable {

              for (i <- recordValue.indices) {

                val value = recordValue(i).asInstanceOf[Map[String, String]]
                val date = value.getOrElse("actionDate", "")
                val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                val timeStamp = fm.parse(date).getTime

                val identifier = timeStamp
                println(s"time is $timeStamp")

                if (i == 0) {

                  VALogger.warn(s"last value is $lastValue")

                  if (identifier > lastValue) {

                    VALogger.warn("identifier : " + identifier + " \n lastValue: " + lastValue)
                    VALogger.warn("Put new identifier to the redis")
                    lazyConn.jedisHset(RedisUtil.REDIS_HASH_NAME, pageUrl, identifier.toString)

                  } else {
                    break()
                  }

                }

                if (identifier <= lastValue) {

                  VALogger.warn("identifier : " + identifier + " \n lastValue: " + lastValue)
                  VALogger.warn("Redis equal value : break")
                  break()

                }

                val userID = value.getOrElse("userID", "")
                val objectID = value.getOrElse("objectID", "")
                val otherID = value.getOrElse("OtherID", "")
                val userName = value.getOrElse("userName", "")
                val title = userName + "的观点: "
                var content = value.getOrElse("body", "")

                if (content.contains("[quote]")) {

                  val text = content.split("[quote]")

                  if (text.nonEmpty) {
                    content = text(0)
                  }

                }

                //过滤掉此类ID
                if (otherID.toInt != 0) {

                  val url = "http://www.taoguba.com.cn/Article" + "/" + objectID + "/" + otherID
                  val stock = ""

                  VALogger.warn(s"Taoguba inserts Data $userID, $title, $url, $timeStamp, $stock")

                  val sqlFlag = DBUtil.insertCall(cstmtArticle, userID, title, 0, 0, url, timeStamp, stock)

                  if (sqlFlag) {
                    inputDataToSql(lazyConn, cstmtDigest, newsMysqlStatement, url, title, timeStamp, content, stopWords, classModels, sentimentModels, keyWordDict, kyConf)
                  } else {
                    VALogger.warn(s"Taoguba first insert has exception $userID, $title, $url, $timeStamp, $stock")
                  }

                } else {
                  VALogger.warn("error report: platform taoguba" + "OtherID:   " + otherID)
                }

              }

            }

          case None => VALogger.error("Parsing failed!")
          case other => VALogger.error("Unknown data structure :" + other)
        }

      }

    } catch {

      case e: Exception =>
        VALogger.exception(e)
    }

    cstmtArticle.close()
    cstmtDigest.close()
    newsMysqlStatement.close()
  }


  def inputDataToSql(lazyConn: LazyConnections,
                     cstmtDigest: CallableStatement,
                     newsMysqlStatement: PreparedStatement,
                     url: String,
                     title: String,
                     time: Long,
                     content: String,
                     stopWords: Array[String],
                     classModels: scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, java.io.Serializable]]],
                     sentimentModels: scala.Predef.Map[scala.Predef.String, scala.Any],
                     keyWordDict: scala.collection.Map[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.Array[scala.Predef.String]]],
                     kyConf: KunyanConf): Unit = {

    var isOk = true

    var digest = ""
    var tempDigest = ""

    if (content != "") {
      tempDigest = DBUtil.getDigest(url, content, lazyConn.summaryConfiguration)
    }

    if (tempDigest == null) {
      isOk = false
    } else {
      digest = DBUtil.getFirstSignData(tempDigest, "\t")
    }

    val summary = DBUtil.interceptData(tempDigest, 300)
    val newDigest = DBUtil.interceptData(digest, 500)
    var categories = ("", "", "")
    var sentiment = ""
    var senti = 1

    try {

      if (tempDigest != "") {

        val words = TextPreprocessing.process(tempDigest, stopWords, kyConf)
        categories = Bayes.predict(words, classModels, keyWordDict)
        sentiment = PredictWithNb.predict(words, sentimentModels, stopWords, kyConf)

        if (sentiment == "neg")
          senti = 0

      } else {
        senti = -1

      }
    } catch {

      case e: Exception =>
        VALogger.exception(e)
        VALogger.error("分词程序异常")
        isOk = false
    }

    if (isOk) {

      var stock = ""

      val newsType = 2

      if (categories._1.length > 0)
        stock = DBUtil.getNewStock(categories._1, keyWordDict("stockDict"))

      stock = DBUtil.getLastSignData(DBUtil.interceptData(stock, 500), "&")
      VALogger.warn(s"Taoguba begins to insert digest to mysql and data to aritcle_info:$digest,$summary,$stock")
      val digestFlag = DBUtil.insertCall(cstmtDigest, url, digest, summary, stock) // 插入article_info digest and summary and stock

      //插入news_info 数据
      if (digestFlag) {

        VALogger.warn(s"Taoguba begins to insert digest to mysql and data to news_info $newsType, $Platform.TAOGUBA.id,$title, $url, $time, $categories._2, $categories._3, $categories._1, $newDigest, $summary, $senti,$Platform.TAOGUBA.toString")
        val newsFlag = DBUtil.insertNewsToMysql(newsMysqlStatement, newsType, Platform.TAOGUBA.id, title, url, time, categories._2, categories._3, categories._1, newDigest, summary, senti, System.currentTimeMillis(), Platform.TAOGUBA.toString)

        if (!newsFlag) {
          VALogger.warn(s"Taoguba begins to insert digest to mysql and data to news_info error")
        }

      } else {
        VALogger.warn(s"Taoguba begins to insert digest to mysql and data to article_info error:$digest,$summary,$stock")
      }

    }

  }

}
