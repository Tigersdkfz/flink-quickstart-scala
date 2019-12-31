//package com.bonc.flink.util;
//
//import redis.clients.jedis.JedisCluster;
//import redis.clients.jedis.ShardedJedis;
//import redis.clients.jedis.ShardedJedisPool;
//
///**
// * @Description: 连接池$
// * @Param: $params$
// * @return: $returns$
// * @Author: ciri
// * @Date: $date$
// */
//public class RedisDataSource {
//    private static ThreadLocal<ShardedJedis> jedisLocal = new ThreadLocal<ShardedJedis>();
//    private static ShardedJedisPool pool;
//    static {
//        pool = JedisPool.getShardedJedisPool();
//    }
//
//    public  ShardedJedis getClient() {
//        JedisCluster jedis = jedisLocal.get();
//        if (jedis == null) {
//            jedis = pool.getResource();
//            jedisLocal.set(jedis);
//        }
//        return jedis;
//    }
//
//    //关闭连接
//    public void returnResource() {
//        ShardedJedis jedis = jedisLocal.get();
//        if (jedis != null) {
//            pool.destroy();
//            jedisLocal.set(null);
//        }
//    }
//}
