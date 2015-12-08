package mysql.redis.replicate.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wens on 15-12-4.
 */
public class RedisUtils {

    public static ShardedJedisPool createSharedJedisPool(GenericObjectPoolConfig config, String redisInfos, String redisPassword) {
        if (config == null) {
            config = new JedisPoolConfig();
        }
        String[] redisAddressInfos = redisInfos.split(",| |(\\r)?\\n");
        List<JedisShardInfo> jdsInfoList = new ArrayList<JedisShardInfo>(redisAddressInfos.length);
        for (String address : redisAddressInfos) {
            if (address.trim().length() == 0) {
                break;
            }
            String[] hostAndPort = address.trim().split(":");
            String host = hostAndPort[0];
            int port = 6379;
            if (hostAndPort.length == 2) {
                port = Integer.parseInt(hostAndPort[1]);
            }
            JedisShardInfo jedisShardInfo = new JedisShardInfo(host, port, 6000);
            if (redisPassword != null && redisPassword.length() != 0) {
                jedisShardInfo.setPassword(redisPassword);
            }
            jdsInfoList.add(jedisShardInfo);
        }
        return new ShardedJedisPool(config, jdsInfoList);
    }


    public static ShardedJedis createSharedJedis(String redisInfos, String redisPassword) {
        String[] redisAddressInfos = redisInfos.split(",| |(\\r)?\\n");
        List<JedisShardInfo> jdsInfoList = new ArrayList<JedisShardInfo>(redisAddressInfos.length);
        for (String address : redisAddressInfos) {
            String[] hostAndPort = address.trim().split(":");
            String host = hostAndPort[0];
            int port = 6379;
            if (hostAndPort.length == 2) {
                port = Integer.parseInt(hostAndPort[1]);
            }
            JedisShardInfo jedisShardInfo = new JedisShardInfo(host, port, 6000);
            if (redisPassword != null && redisPassword.length() != 0) {
                jedisShardInfo.setPassword(redisPassword);
            }
            jdsInfoList.add(jedisShardInfo);
        }
        return new ShardedJedis(jdsInfoList);
    }


    public static void putMap(Jedis client, String key, Map<String, String> data) {
        Pipeline pipelined = client.pipelined();
        for (String column : data.keySet()) {
            pipelined.hset(key, column, data.get(column));
        }
        pipelined.sync();
    }


    public static void returnResource( JedisPool jedisPool ,  Jedis jedis, boolean broken) {
        if (jedis != null) {
            if (broken) {
                jedisPool.returnBrokenResource(jedis);
            } else {
                jedisPool.returnResource(jedis);
            }
        }
    }

    public static void returnResource(ShardedJedisPool shardedJedisPool ,ShardedJedis shardedJedis, boolean broken) {
        if (shardedJedis != null) {
            if (broken) {
                shardedJedisPool.returnBrokenResource(shardedJedis);
            } else {
                shardedJedisPool.returnResource(shardedJedis);
            }
        }
    }

    public static void main(String[] args) {


        ShardedJedis sharedJedis = createSharedJedis("172.16.1.152,172.16.1.153", null);

        String s = sharedJedis.getShard("user_8716184").get("user_8716184");
        System.out.println(s);
    }


}
