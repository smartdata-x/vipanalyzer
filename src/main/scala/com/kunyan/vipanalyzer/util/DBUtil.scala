package com.kunyan.vipanalyzer.util

import com.kunyan.vipanalyzer.db.LazyConnections
import com.kunyan.vipanalyzer.logger.VALogger

/**
  * Created by yang on 5/12/16.
  */
object DBUtil {

  def insertUserInfo(userId: String, followersCount: Int, lazyConn: LazyConnections): Unit = {

    val prep = lazyConn.mysqlConn

    try {

      prep.setString(1, userId)
      prep.setInt(2, followersCount)
      prep.executeUpdate

    } catch {
      case e: Exception =>
        VALogger.error("向MySQL插入数据失败")
        VALogger.exception(e)
    }

  }

}
