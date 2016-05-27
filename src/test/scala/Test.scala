import com.kunyan.vipanalyzer.Scheduler
import com.kunyan.vipanalyzer.config.Platform
import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.parser.MoerParser
import com.kunyan.vipanalyzer.util.{StringUtil, DBUtil}
import org.jsoup.Jsoup

import scala.xml.XML

/**
  * Created by yang on 5/12/16.
  */
object Test extends App {

  val URL = "http://moer.jiemian.com/authorHome.htm?theId=109797660"

//  val html = Jsoup.connect("http://moer.jiemian.com/authorHome.htm?theId=100165518").get.toString

  val configFile = XML.loadFile("/Users/yang/code/SmartData/vipanalyzer/src/main/resources/config.xml")
  val lazyConn = LazyConnections(configFile)
  val result = DBUtil.query(Platform.MOER.id.toString, URL, lazyConn)

  MoerParser.extractUserInfo(result._1, result._2, lazyConn, "test")
}
