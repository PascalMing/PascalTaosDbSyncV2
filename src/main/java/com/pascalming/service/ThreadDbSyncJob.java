package com.pascalming.service;

import com.pascalming.utils.PascalLogger;
import com.pascalming.utils.SpringUtils;
import org.apache.log4j.Logger;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadDbSyncJob
 *
 * @author pascal
 * @date 2021-05-27
 * @description: taos 数据同步作业服务，供RestAPI使用
 */

public class ThreadDbSyncJob extends Thread {
    static Logger logger = Logger.getLogger(ThreadDbSyncSingle.class);
    private String taos_tsBegin;
    private String taos_tsEnd;
    private List<String> listStables;

    private static AtomicInteger dbSyncThreadCount = new AtomicInteger(0);

    public ThreadDbSyncJob(List<String> stables,@NonNull String tsBegin, @NonNull String tsEnd)
    {
        listStables = stables;
        taos_tsBegin = tsBegin;
        taos_tsEnd = tsEnd;
    }
    @Override
    public void run() {
        dbSyncSingleTableThread();
    }

    public static int getAliveThread() {
        return dbSyncThreadCount.get();
    }

    private void dbSyncSingleTableThread() {
        if ( dbSyncThreadCount.get() != 0 )
        {
            PascalLogger.warn(logger,"last job processing, please try later!");
            return;
        }
        dbSyncThreadCount.getAndIncrement();
        try {
            TDengineDbSyncService tDengineDbSyncService = SpringUtils.getObject(TDengineDbSyncService.class);
            tDengineDbSyncService.dbSyncProcessService(listStables,taos_tsBegin,taos_tsEnd);
        }catch (Exception ex)
        {
            PascalLogger.warn(logger,"process err:"+ex.getMessage());
        }finally {
            dbSyncThreadCount.getAndDecrement();
        }
    }
}