package com.atguigu.gmall_publish.service.impl;

import com.atguigu.gmall_publish.mapper.ProductStatsMapper;
import com.atguigu.gmall_publish.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class ProductStatsSericeImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }
}
