package com.huc.utils;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class MyDeserialization implements DebeziumDeserializationSchema<String> {

    /**
     * 反序列化方法
     * 将数据封装成如下格式
     * {
     * "database":"",
     * "tableName":"",
     * "after":{"id":"1001","name":"zs",...},
     * "before":{},
     * "type":"insert"
     * }
     *
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // TODO 1.创建JsonObject对象用来存放最终的结果
        JSONObject result = new JSONObject();

        // TODO 2.获取数据库名和表名
        String[] split = sourceRecord.topic().split("\\.");
        String database = split[1];
        String tableName = split[2];

        // TODO 3.获取before And after
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");

        // todo 获取before数据
        JSONObject beforeJson = new JSONObject();
        // 判断是否有before数据
        if (before != null) {
            Schema schema = before.schema();
            for (Field field : schema.fields()) {
                String name = field.name();
                Object beforevalue = before.get(field);
                beforeJson.put(name, beforevalue);
            }
        }

        // todo 获取after数据
        JSONObject afterJson = new JSONObject();
        // 判断是否有after数据
        if (after != null) {
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                afterJson.put(field.name(), after.get(field));
            }
        }

        // todo 获取操作类型 delete insert update
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();

        if ("create".equals(type) || "read".equals(type)) {
            type = "insert";
        }

        result.put("database", database);
        result.put("tableName", tableName);
        result.put("after", afterJson);
        result.put("before", beforeJson);
        result.put("type", type);

        // 返回最终结果
        collector.collect(result.toJSONString());
    }

    /**
     * 返回的类型
     *
     * @return
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
