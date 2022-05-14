package com.huc.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// 使当前注解作用在字段上
@Target(ElementType.FIELD)  // 指当前注解作用在字段上
// 使当前注解在运行时生效
@Retention(RetentionPolicy.RUNTIME)  // 指当前注解在运行时生效
public @interface TransientSink {
}
