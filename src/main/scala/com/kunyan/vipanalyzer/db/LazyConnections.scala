package com.kunyan.vipanalyzer.db

import java.sql.{Connection, DriverManager}

import com.kunyan.vipanalyzer.logger.VALogger
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.xml.Elem

/**
  * Created by yangshuai on 2016/5/11.
  */
class LazyConnections(createHbaseConnection: () => org.apache.hadoop.hbase.client.Connection) extends Serializable {

  lazy val hbaseConnection = createHbaseConnection()

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

    new LazyConnections(createHbaseConnection)

  }
}


