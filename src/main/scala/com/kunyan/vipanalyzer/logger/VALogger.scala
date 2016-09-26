package com.kunyan.vipanalyzer.logger

import org.apache.log4j.{BasicConfigurator, Logger, PropertyConfigurator}

/**
  * Created by yangshuai on 2016/5/11.
  */
object VALogger {

  val logger = Logger.getLogger("VIP analyzer")

  BasicConfigurator.configure()
  PropertyConfigurator.configure("/home/vip/conf/log4j.properties")

  def exception(e: Exception) = {
    logger.error(e.getLocalizedMessage)
    logger.error(e.getStackTraceString)
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def warn(msg: String): Unit = {
    //    logger.warn(msg)
    println(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

}
