package com.pandatv.work;

import com.pandatv.tools.HiveJDBCConnect;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/21.
 */
public class ConnectTest {
    private static final String hiveDir = "/tmp/hive/likaiqing/panda_realtime/";
    private static final String filepar = "20161120";
    private static final String sep = "\u0001";

    public static void main(String[] args) {
       HiveJDBCConnect hive = new HiveJDBCConnect();
        List<String> userSubList= new ArrayList<String>();
        userSubList.add("a"+sep+"b"+sep+"c"+sep+"d");
        hive.write2(hiveDir+ "panda_user_follow/" + filepar + "/", userSubList);
    }
}
