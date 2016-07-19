package com.kunyan.vipanalyzer.parser.article

import com.kunyan.vipanalyzer.logger.VALogger
import org.jsoup.Jsoup
import scala.collection.mutable.ListBuffer

/**
  * Created by niujiaojiao on 2016/5/27.
  * 淘股吧正文解析
  */
object TaoGuBaContent {

  /**
    * 淘股吧正文解析
    *
    * @param html 将要解析的字符串
    * @return 提取的标题，正文，图片链接
    */
  def getContent(html: String): (String, String, ListBuffer[String]) = {

    try {

      var pictureBuffer = new ListBuffer[String]()
      val doc = Jsoup.parse(html, "UTF-8")
      var title = doc.select("head").select("title").text

      if (title.contains("_淘股吧")) {
        title = title.replace("_淘股吧", "")
      }

      val content = doc.getElementsByAttributeValue("class", "p_coten").text()
      val imgStr = doc.getElementsByAttributeValue("class", "p_wenz").get(0).select("img")

      if (imgStr != null) {
        //有的可能没有图片

        for (i <- 0 until imgStr.size) {

          var pictureUrl = imgStr.get(i).attr("src")

          if (!pictureUrl.startsWith("http://")) {
            pictureUrl = imgStr.get(i).attr("data-original")
          }

          if (!pictureUrl.startsWith("http://css.taoguba")) {
            val img = pictureUrl
            pictureBuffer += img.toString
          }

        }

      }

      (title, content, pictureBuffer)
    } catch {
      case e: Exception =>
        VALogger.exception(e)
        null
    }

  }

}
