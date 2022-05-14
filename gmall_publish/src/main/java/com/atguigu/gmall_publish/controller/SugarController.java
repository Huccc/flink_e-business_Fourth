package com.atguigu.gmall_publish.controller;

import com.atguigu.gmall_publish.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

@RestController
@RequestMapping("/sugar/api")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        // 查询clickhouse数据
        BigDecimal gmv = productStatsService.getGmv(date);

        // 封装JSON并返回

        String json = "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + gmv + " " +
                "}";
        return json;
    }

    @RequestMapping("/test")
    public String test1() {
        return "success";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String format = sdf.format(ts);

        return Integer.parseInt(format);
    }

}
