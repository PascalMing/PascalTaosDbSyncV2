package com.pascalming.service;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * TDengineDbDruidService
 *
 * @author pascal
 * @date 2021-05-27
 * @description: taos 连接池管理
 */

@Repository
public class TDengineDbDruidService {
    private static Logger logger = Logger.getLogger(TDengineDbDruidService.class);
    private static Map<String,DruidDataSource> mapDataSource=new HashMap<>();

    @Value("${iot.dbsync.taos.source.driver}")
    private String source_driver;

    //taos数据源 source
    @Value("${iot.dbsync.taos.source.broker}")
    private String source_broker;

    @Value("${iot.dbsync.taos.source.user}")
    private String source_user;

    @Value("${iot.dbsync.taos.source.password}")
    private String source_password;

    @Value("${iot.dbsync.taos.source.taosdb}")
    private String source_taosdb;

    //taos数据源 destination
    @Value("${iot.dbsync.taos.destination.driver}")
    private String destination_driver;

    @Value("${iot.dbsync.taos.destination.broker}")
    private String destination_broker;

    @Value("${iot.dbsync.taos.destination.user}")
    private String destination_user;

    @Value("${iot.dbsync.taos.destination.password}")
    private String destination_password;

    @Value("${iot.dbsync.taos.destination.taosdb}")
    private String destination_taosdb;

    @Value("${iot.dbsync.taos.initialSize:5}")
    private int taos_initialSize;

    @Value("${iot.dbsync.taos.minIdle:5}")
    private int taos_minIdle;

    @Value("${iot.dbsync.taos.maxActive:100}")
    private int taos_maxActive;

    @Value("${iot.dbsync.taos.maxWait:300000}")
    private int taos_maxWait;

    //获取源数据库连接
    public Connection getSourceConn() throws SQLException {
        return getConnByDruid(source_driver,source_broker, source_taosdb,source_user,source_password);
    }
    //获取目标数据库连接
    public Connection getDestinationConn() throws SQLException {
        return getConnByDruid(destination_driver,destination_broker, destination_taosdb,destination_user,destination_password);
    }
    //从连接池获取连接
    private Connection getConnByDruid(String driver,String broker, String db,String user,String passowrd) throws SQLException {
        String key = driver+broker+db+user;
        DruidDataSource dataSource = mapDataSource.get(key);
        if ( dataSource == null )
        {
            dataSource = new DruidDataSource();
            String jdbc="jdbc:TAOS://";
            if ( driver.equals("com.taosdata.jdbc.rs.RestfulDriver"))
                jdbc = "jdbc:TAOS-RS://";
            String jdbcUrl = jdbc + broker + "/"+db;
            // jdbc properties
            dataSource.setDriverClassName(driver);
            dataSource.setUrl(jdbcUrl);
            dataSource.setUsername(user);
            dataSource.setPassword(passowrd);
            // pool configurations
            dataSource.setInitialSize(taos_initialSize);
            dataSource.setMinIdle(taos_minIdle);
            dataSource.setMaxActive(taos_maxActive);
            dataSource.setMaxWait(taos_maxWait);
            dataSource.setValidationQuery("select server_status()");
            mapDataSource.put(key,dataSource);
        }

        Connection  connection = dataSource.getConnection(); // get connection
        return  connection;
    }
}
