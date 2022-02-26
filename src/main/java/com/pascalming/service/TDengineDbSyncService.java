package com.pascalming.service;

import com.pascalming.domain.TaosStable;
import com.pascalming.domain.TaosTbName;
import com.pascalming.utils.PascalLogger;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * TDengineDbSyncService
 *
 * @author pascal
 * @date 2021-05-27
 * @description: taos 数据同步处理服务，支持命令行和服务运行RestAPI调用两种模式
 * 处理流程：1、读取源库超级表
 *         2、读取每个超级表下的子表
 *         3、创建目标库超级表和子表
 *         4、单表批量插入数据
 */

@Service
public class TDengineDbSyncService implements ApplicationContextAware {
    static Logger logger = Logger.getLogger(TDengineDbSyncService.class);
    static boolean syncInProcessing = false;

    @Value("${iot.dbsync.taos.maxSyncThread}")
    private int taos_maxSyncThread;

    @Value("${iot.servermode}")
    private boolean iot_servermode;

    @Value("${iot.dbsync.taos.tsBegin}")
    public String taos_tsBegin;

    @Value("${iot.dbsync.taos.tsEnd}")
    public String taos_tsEnd;

    @Value("${iot.dbsync.taos.source.stables}")
    public List<String> source_stables;

    @Value("${iot.dbsync.taos.batchcount}")
    public int taos_batchCount;

    @Autowired
    private TDengineDbDruidService tDengineDbDruidService;

    @Autowired
    private TDengineDbService tDengineDbService;

    private ApplicationContext context;

    Map<String, TaosStable> mapStable = new HashMap<>();
    List<TaosTbName> listTbname = new ArrayList<>();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    @Bean
    public void dbSyncBeanService()
    {
        if ( iot_servermode == true )
            return;

        dbSyncProcessService(source_stables,taos_tsBegin,taos_tsEnd);

        ConfigurableApplicationContext ctx = (ConfigurableApplicationContext) context;
        ctx.close();
        System.exit(0);
    }

    //单个超级表同步处理
    private void dbSyncSingleStableProcess(TaosStable stable,String tsBegin,String tsEnd) throws SQLException
    {
        syncInProcessing = true;
        long ltsBegin = (new Date()).getTime();
        try {
            PascalLogger.info(logger,"process stable:" + stable.name);
            long sourceCount = tDengineDbService.dbGetTableRowsCount(true, stable.name, stable.Field.get(0),tsBegin,tsEnd);
            long destinationCount = tDengineDbService.dbGetTableRowsCount(false, stable.name, stable.Field.get(0),tsBegin,tsEnd);
            stable.sourceCount = sourceCount;
            stable.destinationCount = destinationCount;
            if ( destinationCount >= sourceCount)
            {
                PascalLogger.warn(logger,"stable :"+ stable.name+",SourceCount:"+sourceCount+",DestinationCount:"+destinationCount+",ignore!");
                return;
            }
            //PascalLogger.warn(logger,"stable :"+ stable.name+",SourceCount:"+sourceCount+",DestinationCount:"+destinationCount+",sync now!");

            //并行处理单个子表数据,最大并行数：taos_maxSyncThread
            for(int index=0; index < listTbname.size();) {
                int thCount = ThreadDbSyncSingle.getAliveThread();
                if ( thCount >= taos_maxSyncThread ){
                    Thread.sleep(10);
                    continue;
                }
                ThreadDbSyncSingle t1 = new ThreadDbSyncSingle(tsBegin,tsEnd,tDengineDbService,tDengineDbDruidService, stable,listTbname.get(index));
                t1.taos_batchCount = taos_batchCount;
                t1.start();
                index ++;
            }

            long tsProcess = (new Date()).getTime()-ltsBegin;
            PascalLogger.info(logger,"stable :"+ stable.name+",SourceCount:"+sourceCount+",DestinationCount:"+destinationCount+",InsertCount:"+stable.insertCount+",duration:"+tsProcess/1000.0+" s,process finish!");
        }catch (Exception ex)
        {
            PascalLogger.warn(logger,ex.getMessage());
        }finally {
            syncInProcessing = false;
        }
    }

    //加载超级表数据信息
    public void dbLoadStableInfo(List<String> listStables) throws Exception
    {
        //加载源数据库超级表信息
        PascalLogger.info(logger,"show stables;");
        Connection connFrom = tDengineDbDruidService.getSourceConn();
        Statement stmt = connFrom.createStatement();
        stmt.execute("show stables;");
        ResultSet rsRecord = stmt.getResultSet();

        while (rsRecord.next()) {
            TaosStable stable = new TaosStable();
            stable.name = rsRecord.getString("name");
            stable.created_time = rsRecord.getTimestamp("created_time").toString();
            stable.columns = rsRecord.getInt("columns");
            stable.tags = rsRecord.getInt("tags");
            stable.tables = rsRecord.getInt("tables");

            if ( listStables != null && listStables.size()  > 0 )
            {
                if ( listStables.indexOf(stable.name) == -1 )
                    continue;
            }
            {
                Connection conn = tDengineDbDruidService.getSourceConn();
                Statement st = connFrom.createStatement();
                stmt.execute(" DESCRIBE "+stable.name );
                ResultSet rs = stmt.getResultSet();

                while (rs.next()) {
                    stable.Field.add(rs.getString("Field"));
                    stable.Type.add(rs.getString("Type"));
                    stable.Length.add(rs.getInt("Length"));
                    stable.Note.add(rs.getString("Note"));
                }
                rs.close();
                st.close();
                conn.close();
            }
            mapStable.put(stable.name, stable);

            PascalLogger.info(logger,stable.toString());
        }
        rsRecord.close();
        stmt.close();

        PascalLogger.info(logger,"finish, load stable:"+mapStable.size());
    }

    //加载超级表下的数据表信息
    private void dbLoadSingleTableInfo() throws Exception
    {
        Connection connFrom = tDengineDbDruidService.getSourceConn();
        for (TaosStable stable : mapStable.values()) {
            int tableCount = 0;
            Statement stmt = connFrom.createStatement();
            String sql = "select tbname";
            for (int i = 0; i < stable.Field.size(); i ++ )
            {
                if (!stable.Note.get(i).equals("TAG") )
                    continue;
                sql = sql +","+ stable.Field.get(i);
            }

            sql = sql + " from " + stable.name;
            logger.debug(sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                TaosTbName tbName = new TaosTbName();
                tbName.stable_name = stable.name;
                tbName.columns = stable.columns;
                tbName.tbname = rs.getString("tbname");
                for (int i = 0; i < stable.Field.size(); i ++ )
                {
                    if (!stable.Note.get(i).equals("TAG") )
                        continue;
                    tbName.FieldValue.add(rs.getString(stable.Field.get(i)));
                }
                listTbname.add(tbName);
                tableCount ++;
            }
            rs.close();
            stmt.close();
            PascalLogger.info(logger,"stable: "+stable.name+",tables:"+stable.tables+",read tables:"+tableCount);
        }
        connFrom.close();
    }

    private void dbCreateDestinationStable(TaosStable stable) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS "+ stable.name;
        String sqlV = " (";
        String sqlTags = " TAGS(";

        for (int i = 0; i < stable.Field.size(); i ++ )
        {
            String def = stable.Field.get(i) + " " + stable.Type.get(i);
            if ( stable.Type.get(i).equals("BINARY") || stable.Type.get(i).equals("NCHAR"))
                def = def + "("+stable.Length.get(i)+")";
            if ( stable.Note.get(i).equals("TAG"))
            {
                if ( sqlTags.length() == 6 )
                    sqlTags = sqlTags + def;
                else
                    sqlTags = sqlTags + "," + def;
            }
            else
            {
                if ( sqlV.length() == 2 )
                    sqlV = sqlV + def;
                else
                    sqlV = sqlV + "," + def;
            }
        }

        sqlV = sqlV + ")";
        sqlTags = sqlTags + ")";
        sql = sql + sqlV + sqlTags;
        Connection conn = null;
        try {
            conn = tDengineDbDruidService.getDestinationConn();
            tDengineDbService.taosJdbcUpdata(conn,sql);
        } catch (Exception e) {
            PascalLogger.warn(logger,sql+", Err:"+e.getMessage());
        }
        finally {
            if ( conn != null )
                conn.close();
        }
    }
    //数据同步处理服务
    public void dbSyncProcessService(List<String> listStables, @NonNull String tsBegin,@NonNull String tsEnd) {
        PascalLogger.info(logger,"dbSyncProcessService start!");
        long ltsBegin = (new Date()).getTime();
        if ( listStables == null )
            listStables = source_stables;

        try {
            dbLoadStableInfo(listStables);
            dbLoadSingleTableInfo();

            for(TaosStable stable: mapStable.values())
            {
                dbCreateDestinationStable(stable);
                dbSyncSingleStableProcess(stable,tsBegin,tsEnd);
            }
            //等待所有线程数据处理完成
            while (ThreadDbSyncSingle.getAliveThread() > 0 ) {
                Thread.sleep(10);
                continue;
            }
        }catch (Exception ex)
        {
            PascalLogger.warn(logger,"Exception dbSyncProcessService:"+ex.getMessage());
        }
        long tsProcess = (new Date()).getTime()-ltsBegin;
        PascalLogger.info(logger,"dbSyncProcessService process finish,duration(s):"+tsProcess/1000.0);
        SummaryInfo();
    }

    //输出概要信息
    public void SummaryInfo()
    {
        PascalLogger.info(logger,"--SummaryInfo--");
        for (TaosStable stable:mapStable.values()){
            long destinationCount = tDengineDbService.dbGetTableRowsCount(false, stable.name, stable.Field.get(0),taos_tsBegin,taos_tsEnd);
            PascalLogger.info(logger,"name:"+stable.name+",tables:"+stable.tables+",sourceCount:"+stable.sourceCount+",destinationInitCount:"+stable.destinationCount+",insertCount:"+stable.insertCount+",destinationEndCount:"+destinationCount);
        }
    }
}
