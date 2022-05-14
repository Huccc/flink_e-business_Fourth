package com.huc.utils;

import com.huc.bean.TransientSink;
import com.huc.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouseSink(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        try {
                            // 通过反射的方式获取Javabean的所有字段
                            Class<?> clz = t.getClass();
                            // 所有字段
                            Field[] fields = clz.getDeclaredFields();

                            int flag = 0;
                            for (int i = 0; i < fields.length; i++) {
                                // 获取字段
                                Field field = fields[i];

                                // 设置该字段的访问权限
                                field.setAccessible(true);

                                // todo 获取字段注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    flag++;
                                    continue;
                                }

                                // 获取该字段对印的值信息
                                Object value = field.get(t);

                                // 给预编译sql占位符赋值
                                preparedStatement.setObject(i + 1 - flag, value);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                },
                // 配置批量操作
                new JdbcExecutionOptions.Builder()
                        // 凑齐五条数据提交一次
                        .withBatchSize(5)
                        // 或者到一秒钟提交一次
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
