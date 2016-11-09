package com.pandatv.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by likaiqing on 2016/11/9.
 */
public class HiveJDBCConnect {
    private static final Logger logger = LoggerFactory.getLogger(HiveJDBCConnect.class);
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static String url = "jdbc:hive2://10.110.19.9:10000/panda_realtime";
    private static String user = "bigdata";
    private static String password = "cHjhAFeSDpBMrkxyOPzp";
    private static String sql = "";
    private static ResultSet res;

    private static String path = "/bigdata/hive/panda_realtime/";

    public Connection getConn(Properties info) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        // HiveConnection conn = (HiveConnection)
        // DriverManager.getConnection(url, user, password);
        if (!info.containsKey("user")) {
            info.put("user", user);
        }
        if (!info.containsKey("password")) {
            info.put("password", password);
            ;
        }
        info.put("hiveconf:hive.tez.container.size", "4096");
        Connection conn = DriverManager.getConnection(url, info);
        return conn;
    }

    public Connection getConn() throws ClassNotFoundException, SQLException {

        return getConn(new Properties());
    }

    public void write2(String path, List<String> list) {
        path = (path.endsWith("/")) ? path : (path + "/");
//        Configuration conf = new Configuration();
//        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
//        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("core-site.xml"));
//        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
////        conf.set("fs.hdfs.impl",
////                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
////        conf.set("fs.file.impl",
////                org.apache.hadoop.fs.LocalFileSystem.class.getName()
////        );
//        org.apache.hadoop.fs.FileSystem fs;
//        try {
//            fs = org.apache.hadoop.fs.FileSystem.get(conf);
//            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
//            FSDataOutputStream hdfsOutStream = null;
//            CompressionCodec codec = factory.getCodecByName("DEFLATE");
//            Compressor compressor = CodecPool.getCompressor(codec, conf);
//            CompressionOutputStream cmpOut = null;
//            String filepath = path + System.currentTimeMillis() + "_" + Math.random() + ".deflate";
//            if (fs.exists(new Path(filepath))) {
//                hdfsOutStream = fs.append(new Path(filepath));
//            } else {
//                hdfsOutStream = fs.create(new Path(filepath));
//            }
//
//            cmpOut = codec.createOutputStream(hdfsOutStream, compressor);
//            for (String subStr : list) {
//                byte[] line = (subStr + "\n").getBytes("UTF-8");
//                if (line == null)
//                    continue;
//                cmpOut.write(line);
//            }
//            logger.info("write file:{} success,size:{}",filepath,list.size());
//            cmpOut.finish();
//            cmpOut.close();
//            hdfsOutStream.close();
//            fs.close();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }


    public void close(PreparedStatement pst, Connection conn) {
        try {
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.warn("Failed close hive connection ");
        }
    }


    public static ResultSet getResult(String sql) {
        HiveJDBCConnect hive = new HiveJDBCConnect();
        Connection conn = null;
        PreparedStatement pst = null;
        Properties info = new Properties();
        info.put("tez.queue.name", "default");
        info.put("hiveconf:mapred.reduce.tasks", "20");
        info.put("hiveconf:hive.tez.container.size", "2000");
        ResultSet resultSet = null;
        try {
            conn = hive.getConn(info);
            pst = conn.prepareStatement(sql);
            resultSet = pst.executeQuery();
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }
}
