package com.kunyan.vipanalyzer.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by Administrator on 2016/1/7.
  */
object RedisUtil {

  val REDIS_HASH_NAME = "vip:homepage"

  val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxWaitMillis(10000)
  config.setMaxIdle(10)
  config.setMaxTotal(1024)
  config.setTestOnBorrow(true)

  def getRedis(ip: String, port: Int, auth: String, dataBase: Int): JedisPool = {
    new JedisPool(config, ip, port, 20000, auth, dataBase)
  }

}
