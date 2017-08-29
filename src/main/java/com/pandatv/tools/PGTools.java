package com.pandatv.tools;

import com.pandatv.common.PandaProcessor;
import com.pandatv.pojo.AnJuKeLouPan;
import com.pandatv.pojo.LianJiaLouPan;

import java.sql.*;
import java.util.Set;

/**
 * Created by likaiqing on 2017/8/22.
 */
public class PGTools {

    public static void testConn(String[] args) {
        Connection conn = getConn();
        System.out.println(conn);
    }

    public static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://172.17.235.86:5432/crawler";
//            String url = "jdbc:postgresql://localhost:5432/crawler";
            try {
                conn = DriverManager.getConnection(url, "likaiqing", "ff3atkrrr0YThgcjqkdyvzijnpicyb");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void insertAnJuKeLouPan(Set<AnJuKeLouPan> set) {
        String sql = "insert into anjuke_loupan(id,city,district,index,pageno,name,status,pricetext,pricestr,otherpricestr,unit,intprice,intotherprice,aroundpricestr,intaroundprice,advantage,ajust,location,opendate,opendateformat,closedate,closedateformat,lastactiontime,lastactiontitle,lastactioncontent,url,randomstr,pardate) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConn();
            ps = conn.prepareStatement(sql);
            int i=1;
            for (AnJuKeLouPan obj : set) {
                ps.setString(1, obj.getId());
                ps.setString(2, obj.getCity());
                ps.setString(3, obj.getDistrict());
                ps.setInt(4, obj.getIndex());
                ps.setInt(5, obj.getPageNo());
                ps.setString(6, obj.getName());
                ps.setString(7, obj.getStatus());
                ps.setString(8, obj.getPriceText());
                ps.setString(9, obj.getPriceStr());
                ps.setString(10, obj.getOtherPriceStr());
                ps.setString(11, obj.getUnit());
                ps.setInt(12, obj.getIntPrice());
                ps.setInt(13, obj.getIntOtherPrice());
                ps.setString(14, obj.getAroundPriceStr());
                ps.setInt(15, obj.getIntAroundPrice());
                ps.setString(16, obj.getAdvantage());
                ps.setString(17, obj.getAjust());
                ps.setString(18, obj.getLocation());
                ps.setString(19, obj.getOpenDate());
                ps.setString(20, obj.getOpenDateFormat());
                ps.setString(21, obj.getCloseDate());
                ps.setString(22, obj.getCloseDateFormat());
                ps.setString(23, obj.getLastActionTime());
                ps.setString(24, obj.getLastActionTitle());
                ps.setString(25, obj.getLastActionContent());
                ps.setString(26, obj.getUrl());
                ps.setString(27, PandaProcessor.getRandomStr());
                ps.setString(28, PandaProcessor.date);
                ps.addBatch();
                if (i++%100==0){
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, ps);
        }
        set.clear();
        PandaProcessor.writeSuccess = true;
    }

    public static void insertLianJiaLouPan(Set<LianJiaLouPan> set) {
        String sql = "insert into lianjia_loupan(id,city,district,index,pageno,name,status,pricetext,pricestr,otherpricestr,unit,intprice,intotherprice,aroundpricestr,intaroundprice,advantage,ajust,location,opendate,opendateformat,closedate,closedateformat,lastactiontime,lastactiontitle,lastactioncontent,url,randomstr,othername,updatetimestr,daysago,hoursetype,pardate) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConn();
            int i=1;
            ps = conn.prepareStatement(sql);
            for (LianJiaLouPan obj : set) {
                ps.setString(1, obj.getId());
                ps.setString(2, obj.getCity());
                ps.setString(3, obj.getDistrict());
                ps.setInt(4, obj.getIndex());
                ps.setInt(5, obj.getPageNo());
                ps.setString(6, obj.getName());
                ps.setString(7, obj.getStatus());
                ps.setString(8, obj.getPriceText());
                ps.setString(9, obj.getPriceStr());
                ps.setString(10, obj.getOtherPriceStr());
                ps.setString(11, obj.getUnit());
                ps.setInt(12, obj.getIntPrice());
                ps.setInt(13, obj.getIntOtherPrice());
                ps.setString(14, obj.getAroundPriceStr());
                ps.setInt(15, obj.getIntAroundPrice());
                ps.setString(16, obj.getAdvantage());
                ps.setString(17, obj.getAjust());
                ps.setString(18, obj.getLocation());
                ps.setString(19, obj.getOpenDate());
                ps.setString(20, obj.getOpenDateFormat());
                ps.setString(21, obj.getCloseDate());
                ps.setString(22, obj.getCloseDateFormat());
                ps.setString(23, obj.getLastActionTime());
                ps.setString(24, obj.getLastActionTitle());
                ps.setString(25, obj.getLastActionContent());
                ps.setString(26, obj.getUrl());
                ps.setString(27, PandaProcessor.getRandomStr());
                ps.setString(28, obj.getOtherName());
                ps.setString(29, obj.getUpdateTimeStr());
                ps.setInt(30, obj.getDaysAgo());
                ps.setString(31, obj.getHourseType());
                ps.setString(32, PandaProcessor.date);
                ps.addBatch();
                if (i++%100==0){
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, ps);
        }
        set.clear();
        PandaProcessor.writeSuccess = true;
    }

    private static void close(Connection conn, PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
