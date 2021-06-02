package com.pascalming.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * PascalLogger
 *
 * @author pascal
 * @date 2021-06-01
 * @description:简单日志封装，用于WS转发
 */

public class PascalLogger {
    static Queue<String> stringDeque = new ConcurrentLinkedQueue<>();
    static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static private void wsMsg(String type,Object message)
    {
        try {
            stringDeque.add(df.format(new Date())+type+message.toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    static public String getMsg()
    {
        return stringDeque.poll();
    }
    static public void info(org.apache.log4j.Logger logger,Object message) {
        wsMsg(" INFO ",message);
        logger.info(message);
    }
    static public void debug(org.apache.log4j.Logger logger,Object message)
    {
        wsMsg(" DEBUG ",message);
        logger.debug(message);
    }
    static public void warn(org.apache.log4j.Logger logger,Object message) {
        wsMsg(" WARN ",message);
        logger.warn(message);
    }

    static public void error(org.apache.log4j.Logger logger,Object message)
    {
        wsMsg(" ERROR ",message);
        logger.error(message);
    }
    static public void fatal(org.apache.log4j.Logger logger,Object message) {
        wsMsg(" FATAL ",message);
        logger.fatal(message);
    }
}
