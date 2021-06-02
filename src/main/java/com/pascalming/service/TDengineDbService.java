package com.pascalming.service;

import com.pascalming.domain.TaosTbName;
import com.pascalming.utils.PascalLogger;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;

import java.sql.*;

/**
 * TDengineDbService
 *
 * @author pascal
 * @date 2021-05-27
 * @description: taos 数据库处理服务
 */

@Repository
public class TDengineDbService {
    static Logger logger = Logger.getLogger(TDengineDbService.class);

    @Autowired
    private TDengineDbDruidService tDengineDbDruidService;

    @Value("${iot.dbsync.taos.updataRetry:3}")
    private int taos_updataRetry;

    //获取Where时间区间SQL子语句
    public String dbGetWhereTsSubSql(@NonNull String tsField, @NonNull String tsBegin,@NonNull String tsEnd)
    {
        String sql = "";
        if ( tsBegin.length()+tsEnd.length() > 0 ) {
            sql = sql +" where ";
            if ( tsBegin.length() > 0 ) {
                sql = sql + tsField + ">='" + tsBegin + "'";
                if ( tsEnd.length() > 0 )
                    sql = sql + " and " + tsField + " <'" + tsEnd + "'";
            }else
            {
                sql = sql + tsField + " <'" + tsEnd + "'";
            }
        }
        return sql;
    }

    //读取Stable/Table指定时间区间内的记录数
    public long dbGetTableRowsCount(boolean isSource,@NonNull String table,@NonNull String tsField,@NonNull String tsBegin,@NonNull String tsEnd)
    {
        long rsCount = 0;
        Connection conn = null;
        try {
            if ( isSource == true )
                conn = tDengineDbDruidService.getSourceConn();
            else
                conn = tDengineDbDruidService.getDestinationConn();
            Statement stmt = conn.createStatement();
            String sql = "select count(1) from " + table + dbGetWhereTsSubSql(tsField,tsBegin,tsEnd);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                rsCount= rs.getLong(1);
            }
            rs.close();
            stmt.close();
        }catch (Exception ex)
        {}
        finally {
            try {
                if ( conn != null )
                    conn.close();
            }catch (Exception ex)
            {
                PascalLogger.warn(logger,ex.getMessage());
            }
        }
        return rsCount;
    }

    //读取Stable/Table指定时间区间内的记录数
    public String dbGetTableLastRowTs(boolean isSource,@NonNull String table,@NonNull String tsField,@NonNull String tsBegin,@NonNull String tsEnd)
    {
        String ts = tsBegin;
        Connection conn = null;
        try {
            if ( isSource == true )
                conn = tDengineDbDruidService.getSourceConn();
            else
                conn = tDengineDbDruidService.getDestinationConn();
            Statement stmt = conn.createStatement();
            String sql = "select last(*) from " + table + dbGetWhereTsSubSql(tsField,tsBegin,tsEnd);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                ts= rs.getString(1);
            }
            rs.close();
            stmt.close();
        }catch (Exception ex)
        {
            ex.printStackTrace();
        }
        finally {
            try {
                if ( conn != null )
                    conn.close();
            }catch (Exception ex)
            {
                PascalLogger.warn(logger,ex.getMessage());
            }
        }
        return ts;
    }

    public String dbInitSingleTableTagsSql(@NonNull TaosTbName taostb)
    {
        String sql= "INSERT INTO "+taostb.tbname+" USING "+taostb.stable_name +" TAGS (";
        for (int i = 0;i < taostb.FieldValue.size(); i ++ )
        {
            String v = taostb.FieldValue.get(i);
            if ( v == null)
                v = "";
            if ( i == 0 )
                sql = sql +"'"+ v +"'";
            else
                sql = sql +",'"+ v +"'";
        }
        sql = sql + ") VALUES ";
        return sql;
    }

    //插入记录，支持批量插入
    public void taosJdbcUpdata(@NonNull Connection con,@NonNull  String sql){
        int retCode = 0;
        int retry = taos_updataRetry;
        String exMsg = "";
        do {
            try {
                _taosJdbcUpdata(con,sql);
                return;
            }catch (SQLException ex)
            {
                retCode = ex.getErrorCode();
                exMsg = ex.getMessage();
                //连接关闭等不在重试
                if ( retCode == 0)
                    return;
                if ( !(retCode == 866 || retCode == -2147482782) )  {//866 Table does not exist
                   //
                }
            }
            retry --;
            if ( retCode != 0  && retry > 0 ) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }while (retCode != 0 && retry > 0);
        PascalLogger.warn(logger," sql:" + sql.substring(0,60) + ",Code:" + retCode + ",Ex:" + exMsg);
    }
    //插入记录，支持批量插入
    private void _taosJdbcUpdata(@NonNull Connection con,@NonNull String sql) throws SQLException {
        PreparedStatement  ps = null;
        try {
            ps = con.prepareStatement("");
            ps.execute(sql);
        } catch (SQLException ex) {
            throw ex;
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
