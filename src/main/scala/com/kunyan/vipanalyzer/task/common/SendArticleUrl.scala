package com.kunyan.vipanalyzer.task.common

import java.sql.DriverManager
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.util.StringUtil

import scala.xml.XML

/**
  * Created by yangshuai on 2016/5/27.
  */
object SendArticleUrl {

  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile(args(0))

    val lazyConn = LazyConnections(configFile)

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection((configFile \ "mysql" \ "url").text, (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    val sql = s"select * from article_cnfol order by ts desc limit 3000"
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)

    while (result.next()) {
      val url = result.getString("url")
      lazyConn.sendTask((configFile \ "kafka" \ "send").text, StringUtil.getUrlJsonString(Platform.CNFOL.id, url, 0))
    }

    statement.close()
    connection.close()

  }

}
