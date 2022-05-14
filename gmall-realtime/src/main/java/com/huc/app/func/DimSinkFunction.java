package com.huc.app.func;

import com.alibaba.fastjson.JSONObject;
import com.huc.common.GmallConfig;
import com.huc.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    // 声明phoenix连接
    private Connection connection;

    /**
     * 首次执行 用于初始化连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化phoenix连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        // 自动提交参数 MySQL 中为true phoenix 中是false
        // connection.setAutoCommit();
    }

    // value:{"database":"gmall-210826-realtime","tableName":"table_process","after":{"":"","":""},"before":{},"type":"","sinkTable":"dim__","pk":"id"}


    /**
     * 每条数据执行
     * 用于执行语句，保存数据->生成sql语句
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) {
        // todo 拼接插入数据的sql语句  upsert into db.tn(id,name,sex)values(1001,zhangsan,male)
        String upsertSql = getUpsertSql(value.getString("sinkTable"), value.getJSONObject("after"));

        // todo 预编译SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(upsertSql);

            // TODO 删除redis中的缓存，如果是新增数据，Redis并没有，删也删不了，如果是改变的数据，RedisKey = "DIM: " + tableName + ": " + Pk; Redis中是有缓存的，就可以删除掉。
            DimUtil.delDimInfo(value.getString("tableName"), value.getJSONObject("after").getString("id"));

            // todo 执行写入数据库操作
            preparedStatement.execute();

            // todo 批量写入操作
//            preparedStatement.addBatch();

            // todo 提交
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入数据失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println("插入数据失败！！");
                }
            }
        }

    }

    private String getUpsertSql(String sinkTable, JSONObject data) {
        // 拼接插入数据的sql语句  upsert into db.tn(id,name,sex)values('1001','zhangsan','male')

        // 获取列名和数据
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();


        // todo 方式一：使用StringBuilder
        StringBuilder upsertSQL = new StringBuilder("upsert into ");
        upsertSQL.append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        int i = 0;
        for (String column : columns) {
            i++;
            upsertSQL.append(column);
            if (i < columns.size()) {
                upsertSQL.append(",");
            }
        }

        upsertSQL.append(")values('");

        int j = 0;
        for (Object value : values) {
            j++;
            upsertSQL.append(value);
            if (j < values.size()) {
                upsertSQL.append(',');
            }
        }

        upsertSQL.append("')");

        // todo 方式二：将set集合转为list集合来解
        ArrayList<String> list2 = new ArrayList<>(columns);

        ArrayList<String> list3 = new ArrayList<>();
        list3.addAll(columns);

        // 此解法不确定
        List<String> list = Arrays.asList(columns.toArray(new String[0]));

        // todo 方式三：使用StringUtils.join
        // StringUtils.join  相当于Scala中的makeString
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ")values('" +
                StringUtils.join(values, "','") + "')";
    }
}
