package com.pascalming.controller;

import com.alibaba.fastjson.JSONObject;
import com.pascalming.service.ThreadDbSyncJob;
import com.pascalming.utils.PascalLogger;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * DbSync Controller
 *
 * @author pascal
 * @date 2021-05-27
 * @description: DbSync RestApi处理
 */

@RestController
@RequestMapping
public class DbSyncController
{
    private final static Logger logger = Logger.getLogger(DbSyncController.class);
    private final static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @RequestMapping("/info")
    public String info()
    {
        LinkedHashMap<String,Object> dict = new LinkedHashMap();
        dict.put("info", "Pascal TaosDbSync Services");
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        dict.put("uptime(sec)",bean.getUptime()/1000);
        dict.put("startTime",df.format(bean.getStartTime()));
        dict.put("timestamp", df.format(new Date()));
        dict.put("env",System.getenv());
        return JSONObject.toJSONString(dict);
    }
    @RequestMapping("/dosync")
    public String dosync(long begin, int duration,String stables)
    {
        //1262275200000 2010-01-01 00:00:00
        //4102415999000 2099-12-31 23:59:59
        if( begin <1262275200000l ||  begin > 4102415999000l || duration < 1 )
            return "parameter error!";
        String strBegin = df.format(begin);
        String strEnd = df.format(begin+duration*1000);

        if ( ThreadDbSyncJob.getAliveThread() > 0 )
        {
            PascalLogger.warn(logger,"last job processing, please try later!");
            return "last job processing";
        }
        List<String> list = null;
        if ( stables != null && stables.length()> 0 )
            list = Arrays.asList(stables.split(","));
        ThreadDbSyncJob t1 = new ThreadDbSyncJob(list,strBegin,strEnd);
        t1.start();

        return "process taos data ,Data range["+strBegin+","+strEnd+").";
    }
}
