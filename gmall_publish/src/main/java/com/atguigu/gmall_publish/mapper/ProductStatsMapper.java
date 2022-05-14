package com.atguigu.gmall_publish.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

@Mapper
public interface ProductStatsMapper {
    // 查询的抽象方法
    @Select("select sum(order_amount) from product_stats_211025 where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmv(int date);
}
