package com.atguigu.gmall_publisher.service;

import java.math.BigDecimal;

public interface ProductStatsService {
    BigDecimal getGmv(int date);
}
