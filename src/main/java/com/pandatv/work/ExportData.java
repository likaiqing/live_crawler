package com.pandatv.work;

import com.pandatv.common.Const;
import com.pandatv.tools.ExportExcel;
import com.pandatv.tools.HiveJDBCConnect;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by likaiqing on 2016/12/8.
 */
public class ExportData {
    private static String par_date;
    private static String table;
    private static String tableFields;
    private static String[] tableFieldsArr;
    private static String fieldsAlias;
    private static String[] fieldsAliasArr;

    public static void export2Excel(String[] args) {
        par_date = args[1];
        table = args[2];
        tableFields = args[3];
        fieldsAlias = args[4];
        tableFieldsArr = tableFields.split(",");
        fieldsAliasArr = fieldsAlias.split(",");
        if (tableFieldsArr.length == 1 || tableFieldsArr.length != fieldsAliasArr.length) {
            System.out.println("字段个数必须大于1,且表字段和字段别名保持个数一致");
            System.exit(1);
        }
        List<Map<String, String>> maps = new ArrayList<>();
        HiveJDBCConnect hive = new HiveJDBCConnect();
        Properties info = new Properties();
        info.put("tez.queue.name", "default");
        try {
            hive.getConn(info);
            ResultSet result = hive.excutesql("select " + tableFields + " from " + table + " where par_date='" + par_date + "'");
            OutputStream out = new FileOutputStream(Const.EXPORTEXCELDIR + table.substring(table.indexOf(".") + 1) + par_date + ".xlsx");
            while (result.next()) {
                Map<String, String> fieldMap = new LinkedHashMap<>();
                for (int i = 0; i < tableFieldsArr.length; i++) {
                    String fieldName = tableFieldsArr[i];
                    String fieldRes = result.getString(fieldName);
                    fieldMap.put(fieldName, fieldRes);
                }
                maps.add(fieldMap);
            }
            ExportExcel<Object> ex = new ExportExcel<>();
            ex.exportExcel(table.substring(table.indexOf(".") + 1) + par_date, fieldsAliasArr, maps, out, tableFieldsArr);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        args = new String[]{"20161207", "panda_result.crawler_day_anchor_ana", "rid,name,plat,rec_times", "日期,主播号,平台,推荐次数", "DayAnchor"};
        export2Excel(args);
    }
}
