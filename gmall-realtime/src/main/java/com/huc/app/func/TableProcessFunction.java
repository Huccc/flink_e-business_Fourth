package com.huc.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huc.bean.TableProcess;
import com.huc.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 1.声明Phoenix连接，为了之后建表
    private Connection connection;

    // 2.声明Map状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // todo 初始化Phoenix连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    // 处理广播流的数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 1.解析数据为TableProcess对象，处理JSON套JSON的情况
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            BuildTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        // 写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        // TODO 广播流的Key
        String Key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();

        broadcastState.put(Key, tableProcess);

    }


    // todo 创建Phoenix表
    // create table if not exists db.tn(id varchar primary key,name varchar,sex varchar) xxxx;
    private void BuildTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        PreparedStatement preparedStatement = null;
        try {
            // 构建建表语句
            StringBuilder createtableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {
                // 取出列名
                String column = columns[i];

                // 判断当前字段是否为主键
                if (column.equals(sinkPk)) {
                    createtableSql.append(column)
                            .append(" varchar primary key ");
                } else {
                    createtableSql.append(column)
                            .append(" varchar ");
                }
                // 判断是否为最后一个字段
                if (i < columns.length - 1) {
                    createtableSql.append(",");
                }
            }
            createtableSql.append(")")
                    .append(sinkExtend);

            // 打印建表语句
            System.out.println(createtableSql);

            // 执行预编译sql

            preparedStatement = connection.prepareStatement(createtableSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("创建phoenix表 " + sinkTable + " 失败！！");
        } finally {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 主流flinkcdcDS数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "after":{"id":"1001","name":"zs",...},
     * "before":{},
     * "type":"insert"
     * }
     *
     * @param ctx
     * @param out
     * @throws Exception
     * @`param value
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 取出状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // todo 主流的key
        String Key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(Key);

        if (tableProcess != null) {
            // 过滤字段
            // 侧输出流来的数据 id,dic_name,parent_code,create_time,operate_time
            filterColumns(value.getJSONObject("after"), tableProcess.getSinkColumns());

            // 分为Kafka主流    hbase侧输出流

            // 将表名或者主题名添加到字段中
            value.put("sinkTable", tableProcess.getSinkTable());

            // 添加同一主键字段
            value.put("Pk", tableProcess.getSinkPk());

            String sinkType = tableProcess.getSinkType();

            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            }
        } else {
            System.out.println(Key + "不存在");
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        // 侧输出流
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);
        // 主流
        Set<Map.Entry<String, Object>> entries = data.entrySet();

        // JSONObject 类型 全部当成Map处理
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}

















