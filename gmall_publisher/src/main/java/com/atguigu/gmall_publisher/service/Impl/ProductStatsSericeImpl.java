package com.atguigu.gmall_publisher.service.Impl;

import com.atguigu.gmall_publisher.mapper.ProductStatsMapper;
import com.atguigu.gmall_publisher.service.ProductStatsService;
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
