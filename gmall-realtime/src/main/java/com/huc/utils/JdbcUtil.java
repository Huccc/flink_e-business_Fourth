package com.huc.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.huc.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * select count(*) from t1;                                     一行一列
 * select id from t1;                                           多行一列
 * select id,name from t1 where id =1001; id     是唯一键        一行多列
 * select * from t1;                                            多行多列
 * <p>
 * todo 工具类中的异常一般直接抛
 */
public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        // 创建集合用于存放查询结果
        ArrayList<T> resultList = new ArrayList<>();

        // todo 编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        // 获取列名信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // todo 遍历resultSet
        // next 查看下一个对象，同时指针指向下一步
        while (resultSet.next()) {
            // todo 将每行数据转换为T对象
            T t = clz.newInstance();
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object columnValue = resultSet.getObject(i + 1);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                // todo 给T对象赋值
                BeanUtils.setProperty(t, columnName, columnValue);
            }
            // todo 将T对象放入集合中
            resultList.add(t);
        }

        // 关闭资源对象
        resultSet.close();
        preparedStatement.close();

        // 返回结果
        return resultList;
    }

    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> queryList = queryList(connection, "select * from GMALL211025_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, false);

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}
