package com.kunyan.vipanalyzer.parser.article

import com.kunyan.vipanalyzer.logger.VALogger
import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/5/27.
  * 雪球正文解析
  */
object SnowBallContent {

  /**
    * 雪球正文解析
    *
    * @param html 将要解析的字符串
    * @return 返回标题，正文
    */
  def getContent(html: String): (String, String) = {

    try {

      val doc = Jsoup.parse(html, "UTF-8")
      var title = doc.select("head").select("title").text

      if (title.contains("- 雪球")) {
        title = title.replace("- 雪球", "")
      }

      var content = doc.getElementsByAttributeValue("class", "detail").text()

      (title, content)
    } catch {
      case e: Exception =>
        VALogger.exception(e)
        null
    }

  }

}
