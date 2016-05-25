package com.kunyan.vipanalyzer.db

import java.sql.{DriverManager, PreparedStatement}
import java.util.Properties

import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.logger.VALogger
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.xml.Elem

/**
  * Created by yangshuai on 2016/5/11.
  */
class LazyConnections(createHbaseConnection: () => org.apache.hadoop.hbase.client.Connection,
                      createProducer: () => Producer[String, String],
                      createMySQLConnection: () => PreparedStatement) extends Serializable {

  lazy val hbaseConn = createHbaseConnection()

  lazy val mysqlConn = createMySQLConnection()

  lazy val producer = createProducer()

  def sendTask(topic: String, value: String): Unit = {

    val message = new KeyedMessage[String, String](topic, value)

    try {
      producer.send(message)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def sendTask(topic: String, values: Seq[String]): Unit = {

    val messages = values.map(x => new KeyedMessage[String, String](topic, x))

    try {
      producer.send(messages: _*)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def getTable(tableName: String) = hbaseConn.getTable(TableName.valueOf(tableName))

}

object LazyConnections {

  def apply(configFile: Elem): LazyConnections = {

    val createHbaseConnection = () => {

      val hbaseConf = HBaseConfiguration.create
      hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
      hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)
      VALogger.warn("create connection")

      val connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

      sys.addShutdownHook {
        connection.close()
      }

      VALogger.warn("Hbase connection created.")

      connection
    }

    val createProducer = () => {

      val props = new Properties()
      props.put("metadata.broker.list", (configFile \ "kafka" \ "brokerList").text)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }

    val createCNFOLPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      VALogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO article_cnfol (user_id, title, recommend, time, reproduce, comment, url) VALUES (?,?,?,?,?,?,?)")
    }

    val createMOERPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      VALogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO vip_moer (user_id, followers_count, official_vip, name, introduction, home_page, portrait, field) VALUES (?,?,?,?,?,?,?,?)")
    }

    val createTGBPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      VALogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO article_taoguba (user_id, title, recommend, `read`, comment, url, ts) VALUES (?,?,?,?,?,?,?)")
    }

    val createSnowBallPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      VALogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO temp_article_snowball (user_id, title, retweet, reply, url, ts) VALUES (?,?,?,?,?,?)")
    }

    val createWeiboPs = () => {

      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

      sys.addShutdownHook {
        connection.close()
      }

      VALogger.warn("MySQL connection created.")

      connection.prepareStatement("INSERT INTO vip_weibo (user_id, followers_count, official_vip, name, introduction, home_page, portrait) VALUES (?,?,?,?,?,?,?)")
    }

    new LazyConnections(createHbaseConnection, createProducer, createTGBPs)

  }

}


