package com.bonc.flink.util;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Description: redispool$
 * @Param: $params$
 * @return: $returns$
 * @Author: ciri
 * @Date: $date$
 */
public class JedisPool {
    //JedisPool连一台Redis，ShardedJedisPool连Redis集群，通过一致性哈希算法决定把数据存到哪台上，算是一种客户端负载均衡，
    private static ShardedJedisPool pool;
    private static int MAXTOTAL=300;
    private static int MAXIDLE=200;
    private static int MINIDEL=10;
    private static int MAXWAIRMILLIS=1000;
    private static Boolean TESTONBORROW=true;
    private static Boolean TESTONRETURN=false;
    private static Boolean TESTWHILEIDLE=false;
    //静态代码初始化连接池配置
    static {
        try {
            //初始化连接池参数配置
            JedisPoolConfig config=initConfig();
            List<JedisShardInfo> shards=new ArrayList<JedisShardInfo>();
            String host="172.16.125.163:8001,172.16.125.163:8002,172.16.125.163:8003," +
                    "172.16.125.33:8004,172.16.125.33:8005,172.16.125.33:8006,172.16.22.156:6379";//服务器地址,密码
            Set<String> hosts=init(host);
            for (String hs:hosts){
                String[] values=hs.split(":");
                JedisShardInfo shard=new JedisShardInfo(values[0],Integer.parseInt(values[1]));
                if(values.length>2){
                    shard.setPassword(values[2]);
                }
                shards.add(shard);
            }
            pool=new ShardedJedisPool(config,shards);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    //初始化连接池参数
    private static JedisPoolConfig initConfig(){
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxTotal(MAXTOTAL);
        config.setMaxIdle(MAXIDLE);
        config.setMinIdle(MINIDEL);
        config.setMaxWaitMillis(MAXWAIRMILLIS);
        config.setTestOnBorrow(TESTONBORROW);
        config.setTestOnReturn(TESTONRETURN);
        config.setTestWhileIdle(TESTWHILEIDLE);
        return config;
    }

    private static Set<String> init(String values){
        if(StringUtils.isBlank(values)){
            throw new NullPointerException("redis host not found");
        }
        Set<String> paramter=new HashSet<String>();
        String[] sentinelArray=values.split(",");
        for(String str:sentinelArray){
            paramter.add(str);
        }
        return paramter;
    }

    public static ShardedJedisPool getShardedJedisPool(){
        return pool;
    }
}
