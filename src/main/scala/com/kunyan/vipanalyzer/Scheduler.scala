package com.kunyan.vipanalyzer

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.xml.XML

/**
  * Created by yang on 5/10/16.
  */
object Scheduler {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("VIP ANALYZER")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val path = args(0)

    val configFile = XML.loadFile(path)

    val groupId = (configFile \ "kafka" \ "contentTopic").text
    val brokerList = (configFile \ "kafka" \ "ip").text + ":" + (configFile \ "kafka" \ "brokerList").text
    val topicsSet = groupId.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> groupId)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.map(_._2).filter(_.length > 0).foreachRDD(rdd => {
      rdd.foreach(X => {})
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
