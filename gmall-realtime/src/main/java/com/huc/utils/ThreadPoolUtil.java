package com.huc.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 单例模式：
 * 懒汉式：用的时候再创建对象，缺点是存在线程安全问题
 * 饿汉式：提前将对象创建好，缺点是可能造成资源浪费
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            100,
                            // 线程访问的最长时间
                            100,
                            // 上面一个参数的单位
                            TimeUnit.SECONDS,
                            // 线程队列，排队作用，先进先出
                            // 数据来了，先进队列
                            // 当队列满了（一般情况下，队列的之都很大）才会创建新的线程
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }
}
