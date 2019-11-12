package com.bonc.flink.util

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @Description: redis的工具类
  * @Param: $params$
  * @return: $returns$
  * @Author: ciri
  * @Date: $date$
  */
object RedisClient {

  val clients = RedisConnector.clients

  /**
    * @param key
    * @param value
    * @return
    */
  def set(key: String, value: String): Unit = {
    clients.set(key, value)
  }

  /**
    *
    * @param key
    * @return
    */
  def get(key: String): Option[String] = {
    val value = clients.get(key)
    if (value == null)
      None
    else
      Some(value)
  }


  /**
    *
    * @param key
    */
  def del(key: String): Unit = {
    clients.del(key)
  }

  /**
    *
    * @param hkey
    * @param key
    * @param value
    * @return
    */
  def hset(hkey: String, key: String, value: String): Boolean = {
    clients.hset(hkey, key, value) == 1
  }

  /**
    *
    * @param hkey
    * @param key
    * @return
    */
  def hget(hkey: String, key: String): Option[String] = {

    val value = clients.hget(hkey, key)
    if (value == null)
      None
    else
      Some(value)

  }

  /**
    *
    * @param hkey
    * @param key
    * @return
    */
  def hdel(hkey: String, key: String): Option[Long] = {

    Some(clients.hdel(hkey, key))

  }

  /**
    *
    * @param hkey
    * @param map
    */
  def hmset(hkey: String, map: mutable.Map[String, String]): Unit = {
    clients.hmset(hkey, mapAsJavaMap(map))
  }

  /**
    *
    * @param key
    * @param value
    * @return
    */
  def rpush(key: String, value: String): Option[Long] = {
    Some(clients.rpush(key, value))
  }


  /**
    *
    * @param key
    * @return
    */
  def lpop(key: String): Option[String] = {

    val value = clients.lpop(key)
    if (value == null)
      None
    else
      Some(value)
  }


  /**
    *
    * @param key
    * @return
    */
  def lhead(key: String): Option[String] = {

    val head = clients.lindex(key, 0)
    if (head == null)
      None
    else
      Some(head)
  }

  /**
    *
    * @param key
    * @return
    */
  def incr(key: String): Option[Long] = {
    val inc = clients.incr(key)
    if (inc == null)
      None
    else
      Some(inc)
  }

  /**
    *
    * @param key
    * @param time
    * @return
    */
  def expire(key: String, time: Int) = {
    clients.expire(key, time)
  }

  /**
    *
    * @param key
    * @return
    */
  def ttl(key: String): Option[Long] = {
    Some(clients.ttl(key))
  }

  def main(args: Array[String]): Unit = {

    //clients.set("matest","777")
    //print(clients.get("matest"));
    //print(clients.get("_CACHE_SY_SERV_DICT".getBytes("UTF-8")));
    //print(clients.hget("_CACHE_SY_SERV_DICT","GA_GATEWAY_PROTOCOL"));
    //print(clients.hmget("_CACHE_SY_SERV_DICT","GA_GATEWAY_PROTOCOL"));
    print(clients.mget("age"));
    println()
  }

}

object RedisConnector {

  private val jedisClusterNodes = new util.HashSet[HostAndPort]()
  jedisClusterNodes.add(new HostAndPort("172.16.125.163", 8001))
  jedisClusterNodes.add(new HostAndPort("172.16.125.163", 8002))
  jedisClusterNodes.add(new HostAndPort("172.16.125.163", 8003))
  jedisClusterNodes.add(new HostAndPort("172.16.125.33", 8004))
  jedisClusterNodes.add(new HostAndPort("172.16.125.33", 8005))
  jedisClusterNodes.add(new HostAndPort("172.16.125.33", 8006))
  //jedisClusterNodes.add(new HostAndPort("172.16.22.156",6379))

  //val clients = new JedisCluster(jedisClusterNodes,3000,3000,3,"xingda",new GenericObjectPoolConfig)
  val clients = new JedisCluster(jedisClusterNodes,3000,3000,3,new GenericObjectPoolConfig)

}


/*
object MainClass {
  def main(args: Array[String]): Unit = {
    val dao = RedisClient
    dao.set("lijie", "123")
    //    dao.del("lijie")
    println(dao.get("lijie"))
    //    dao.hset("myhset", "lijie1", "chongqing")
    //    dao.hset("myhset", "lijie2", "shenzheng")
    //    println(dao.hget("myhset", "lijie1"))
    //    dao.hdel("myhset", "lijie1")
    //    dao.rpush("myfifo", "a")
    //    dao.rpush("myfifo", "c")
    //    dao.rpush("myfifo", "b")
    //
    //    println(dao.lhead("myfifo"))
    //
    //    println(dao.lpop("myfifo"))
    //    println(dao.lpop("myfifo"))
    //    println(dao.lpop("myfifo"))

    //    println(dao.incr("myincr"))

  }

}
*/
