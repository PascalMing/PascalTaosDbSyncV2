package com.pascalming.service;

import com.pascalming.domain.TaosStable;
import com.pascalming.domain.TaosTbName;
import com.pascalming.utils.PascalLogger;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadDbSyncSingle
 *
 * @author pascal
 * @date 2021-05-27
 * @description: taos 子表同步服务，支持多线程以提高吞吐量
 */


public class ThreadDbSyncSingle extends Thread {
    static Logger logger = Logger.getLogger(ThreadDbSyncSingle.class);
    private String taos_tsBegin;
    private String taos_tsEnd;
    private TDengineDbService tDengineDbService;
    private TDengineDbDruidService tDengineDbDruidService;

    private static AtomicInteger dbSyncSingleTableThreadCount = new AtomicInteger(0);
    private TaosStable stable;
    private TaosTbName taostb;

    public int taos_batchCount=300;

    @Override
    public void run()
    {
        dbSyncSingleTableThread();
        dbSyncSingleTableThreadCount.getAndDecrement();
    }
    public static int getAliveThread()
    {
        return dbSyncSingleTableThreadCount.get();
    }
    public ThreadDbSyncSingle(String tsBegin,String tsEnd, TDengineDbService tDengineDbService, TDengineDbDruidService tDengineDbDruidService, TaosStable stable, TaosTbName taostb)
    {
        taos_tsBegin = tsBegin;
        taos_tsEnd = tsEnd;
        this.tDengineDbService = tDengineDbService;
        this.tDengineDbDruidService = tDengineDbDruidService;
        this.stable = stable;
        this.taostb = taostb;
        dbSyncSingleTableThreadCount.getAndIncrement();
    }

    /* 同步单个表线程
       1、读取目标表指定时间区间内的总记录数  select count(v) from stableXX.txxxx;
       2、有记录，取最后的时间
       3、读源表指定时间区间内的记录数
       4、同步:第一条，带超级表插入,后续，批量插入
     */
    private void dbSyncSingleTableThread()
    {
        Connection connTo = null;
        Connection connFrom = null;
        Statement stmt = null;
        long tsBegine = (new Date()).getTime();
        long tbRsCount = 0;
        long rsCountSource = 0;
        long rsCountDestination = 0;
        String tags = tDengineDbService.dbInitSingleTableTagsSql(taostb);
        String sql = "";
        StringBuilder values = new StringBuilder();
        try {
            rsCountSource = tDengineDbService.dbGetTableRowsCount(true,taostb.tbname,stable.Field.get(0),taos_tsBegin,taos_tsEnd);
            rsCountDestination = tDengineDbService.dbGetTableRowsCount(false,taostb.tbname,stable.Field.get(0),taos_tsBegin,taos_tsEnd);
            if ( rsCountDestination >= rsCountSource)
            {
                PascalLogger.info(logger,"stable :"+stable.name+"."+taostb.tbname+",SourceCount:"+rsCountSource+",DestinationCount:"+rsCountDestination+",ignore!");
                return;
            }
            String stBegin = taos_tsBegin;
            if ( rsCountDestination > 0 )
            {
                stBegin = tDengineDbService.dbGetTableLastRowTs(false,taostb.tbname,stable.Field.get(0),taos_tsBegin,taos_tsEnd);
            }
            //单表数据同步
            connTo = tDengineDbDruidService.getDestinationConn();
            connFrom = tDengineDbDruidService.getSourceConn();
            stmt = connFrom.createStatement();
            sql = "select * from " + taostb.tbname + tDengineDbService.dbGetWhereTsSubSql(stable.Field.get(0),stBegin,taos_tsEnd);
            PascalLogger.info(logger,sql);
            ResultSet rsRecord = stmt.executeQuery(sql);
            values = new StringBuilder();
            int batchCount = 0;
            while (rsRecord.next()) {
                StringBuilder v = new StringBuilder();
                long ts = rsRecord.getLong(1);
                if ( ts < 4102415999l) //s与ms自动识别，4102415999=2099-12-31 23:59:59
                    ts *=1000;
                v.append(" ("+ts);
                for (int i=1; i < taostb.columns; i ++ ){
                    String vCol = rsRecord.getString(i+1);
                    if (vCol.isEmpty()){
                        v.append(",null");
                    }else {
                        switch (stable.Type.get(i-1))
                        {
                            case "INT":
                            case "BIGINT":
                            case "FLOAT":
                            case "DOUBLE":
                            case "SMALLINT":
                            case "TINYINT":
                            case "BOOL":
                                v.append(","+vCol+"");
                                break;
                            default:
                                v.append(",'"+vCol+"'");
                        }
                    }
                }
                v.append(")");
                values.append(v);
                batchCount ++;
                tbRsCount ++;
                if ( batchCount >= taos_batchCount && values.length() > 0 )
                {
                    batchCount = 0;

                    tDengineDbService.taosJdbcUpdata(connTo,tags+values);
                    values = new StringBuilder();
                    //PascalLogger.info(logger,"insert into "+tagMd5+" tagname:"+tbName.tagname+" count: "+tbRsCount+" Total: "+ rsAllCount);
                }
            }
            if ( batchCount > 0 && values.length() > 0 ){
                tDengineDbService.taosJdbcUpdata(connTo,tags+values);
            }
            long tsProcess = (new Date()).getTime()-tsBegine;
            PascalLogger.info(logger,"stable :"+stable.name+"."+taostb.tbname+",SourceCount:"+rsCountSource+",DestinationCount:"+rsCountDestination+" insertCount:"+tbRsCount+",duration:"+tsProcess/1000.0+" s,sync finish!");
        }catch (Exception ex)
        {
            long tsProcess = (new Date()).getTime()-tsBegine;
            PascalLogger.warn(logger,"stable :"+stable.name+"."+taostb.tbname+",SourceCount:"+rsCountSource+",DestinationCount:"+rsCountDestination+" insertCount:"+tbRsCount+",duration:"+tsProcess/1000.0+" s,process ex:"+ex.getMessage());
        }finally {
            synchronized (logger) {
                stable.insertCount += tbRsCount;
            }
            try {
                if ( stmt != null )
                    stmt.close();
                if ( connFrom != null )
                    connFrom.close();
                if ( connTo != null )
                    connTo.close();
            }catch (Exception ex)
            {
                PascalLogger.warn(logger,ex.getMessage());
            }
        }
    }
}