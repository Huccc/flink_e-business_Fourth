package com.huc.app.func;

import com.alibaba.fastjson.JSONObject;

// 接口里面都是抽象方法
public interface AsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject jsonObject) throws Exception;
}
