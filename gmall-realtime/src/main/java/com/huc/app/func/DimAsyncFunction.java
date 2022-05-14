package com.huc.app.func;

import com.alibaba.fastjson.JSONObject;
import com.huc.common.GmallConfig;
import com.huc.utils.DimUtil;
import com.huc.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 该类继承异步方法类RichAsyncFunction，实现自定义维度查询接口
 * 其中RichAsyncFunction<IN,OUT>是Flink提供的异步方法类，此处因为是查询操作输入类和返回类一致，所以是<T,T>。
 * RichAsyncFunction这个类要实现两个方法:
 * open用于初始化异步连接池。
 * todo     asyncInvoke方法是核心方法，里面的操作必须是异步的，如果你查询的数据库有异步api也可以用线程的异步方法，如果没有异步方法，就要自己利用线程池等方式实现异步查询。
 *
 * @param <T>
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // todo 1.查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));

                    // todo 2.将维度信息补充至数据
                    join(input, dimInfo);

                    // todo 3.将关联好的维度信息的数据输出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }

}
