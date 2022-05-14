package com.huc.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huc.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * 使用Redis作为旁路缓存
 * RedisKey
 * String： table+id
 * RedisKey如何设计     采用String    tableName:id
 * 1.设置过期时间String是针对每一条数据，Hash针对一堆（Redis一般是集群，Hash可能会造成某台机器压力过大）
 * <p>
 * 1.	缓存穿透
 * 2.	缓存击穿
 * 3.	缓存雪崩（大量的key，同一时间生效）
 * <p>
 * todo 缓存要设过期时间，不然冷数据会常驻缓存浪费资源。
 * todo 要考虑维度数据是否会发生变化，如果发生变化要主动清除缓存。
 * <p>
 * 2)	缓存的选型
 * 一般两种：堆缓存或者独立缓存服务(redis，memcache)，
 * 堆缓存，从性能角度看更好，毕竟访问数据路径更短，减少过程消耗。但是管理性差，其他进程无法维护缓存中的数据。
 *  独立缓存服务（redis,memcache）本身性能也不错，不过会有创建连接、网络IO等消耗。但是考虑到数据如果会发生变化，那还是独立缓存服务管理性更强，而且如果数据量特别大，独立缓存更容易扩展。
 * 因为咱们的维度数据都是可变数据，所以这里还是采用Redis管理缓存。
 *
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String Pk) throws Exception {

        // TODO 查询Redis
        Jedis jedis = RedisUtil.getJedis();
        // todo rediskey
        String RedisKey = "DIM: " + tableName + ": " + Pk;

        String resultStr = jedis.get(RedisKey);

        if (resultStr != null) {
            // 重置过期时间
            jedis.expire(RedisKey, 3600 * 24);

            // 归还连接
            jedis.close();

            return JSON.parseObject(resultStr);
        }

        // todo 查询的SQL语句    select * from db.tn where id = '1001';
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + Pk + "'";

        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject result = queryList.get(0);

        // TODO 将数据写入Redis
        jedis.set(RedisKey, result.toJSONString());
        // 设置过期时间
        jedis.expire(RedisKey, 3600 * 24);

        // 归还连接
        jedis.close();

        return result;
    }

    /**
     * 先删Redis数据        再更新Phoenix数据  （也有可能出问题   解决方案：不删了直接更新到redis）
     * Redis删掉之后，如果Phoenix更新失败，Kafka可以重新提交偏移量（可以重复消费）最终数据还是会成功
     * <p>
     * 先更新Phoenix数据    再删Redis数据
     * Phoenix更新后，Redis没有删除成功，会使查询结果不一致
     * <p>
     * 因此可以实行，不删除Redis数据，直接更新Redis数据，在更新Phoenix数据
     *
     * @param tableName
     * @param Pk
     */
    public static void delDimInfo(String tableName, String Pk) {
        // todo rediskey
        String RedisKey = "DIM: " + tableName + ": " + Pk;

        Jedis jedis = RedisUtil.getJedis();
        // todo 删除Redis数据
        jedis.del(RedisKey);

        // 归还连接
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "aa_bb"));
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long time1 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "5"));
        long time2 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "5"));
        long time3 = System.currentTimeMillis();

        System.out.println(time2 - time1);
        System.out.println(time3 - time2);

        // todo 关闭连接
        connection.close();
    }
}
