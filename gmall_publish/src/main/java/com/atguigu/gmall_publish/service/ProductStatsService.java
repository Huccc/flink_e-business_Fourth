package com.atguigu.gmall_publish.service;

import java.math.BigDecimal;

public interface ProductStatsService {
    BigDecimal getGmv(int date);
}
