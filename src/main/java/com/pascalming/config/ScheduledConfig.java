package com.pascalming.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ScheduledConfig
 *
 * @author pascal
 * @date 2021-05-27
 * @description:任务调度器，解决和WebSocket冲突
 */

@Configuration
public class ScheduledConfig {

    private static final int corePoolSize = 100;       		// 核心线程数（默认线程数）
    private static final int maxPoolSize = 500;			    // 最大线程数
    private static final int keepAliveTime = 10;			// 允许线程空闲时间（单位：默认为秒）
    private static final int queueCapacity = 500;			// 缓冲队列数
    private static final String threadNamePrefix = "Async-Service-Sync"; // 线程池名前缀

    @Bean("taskExecutor")
    public ThreadPoolTaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(keepAliveTime);
        executor.setThreadNamePrefix(threadNamePrefix);
        /**
         * 线程池对拒绝任务的处理策略:
         * 1. CallerRunsPolicy ：这个策略重试添加当前的任务，他会自动重复调用 execute() 方法，直到成功。
         *
         * 2. AbortPolicy ：对拒绝任务抛弃处理，并且抛出异常。
         *
         * 3. DiscardPolicy ：对拒绝任务直接无声抛弃，没有异常信息。
         *
         * 4. DiscardOldestPolicy ：对拒绝任务不抛弃，而是抛弃队列里面等待最久的一个线程，然后把拒绝任务加到队列。
         */
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        // 初始化
        executor.initialize();
        return executor;
    }

    @Bean
    public TaskScheduler taskScheduler(){
        ThreadPoolTaskScheduler scheduler=new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(50);
        scheduler.initialize();
        return scheduler;
    }

}
