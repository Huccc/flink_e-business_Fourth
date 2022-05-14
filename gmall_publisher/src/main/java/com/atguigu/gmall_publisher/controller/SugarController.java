package com.atguigu.gmall_publisher.controller;

import com.atguigu.gmall_publisher.service.ProductStatsService;
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

    @RequestMapping("/test")
    public String test1() {
        return "success";
    }

    @RequestMapping("/test1")
    public String test2(@RequestParam(value = "name", defaultValue = "huc") String name) {
        System.out.println(name + " 就是我！！");
        return "success";
    }

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        BigDecimal gmv = productStatsService.getGmv(date);

        if (date == 0) {
            date = getToday();
        }

        String Json = "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + gmv + " " +
                "}";

        return Json;
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String format = sdf.format(ts);

        return Integer.parseInt(format);
    }
}



