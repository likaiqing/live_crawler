package com.pandatv.tools;

import com.pandatv.pojo.DetailAnchor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;


public class HiveJDBCConnect {

    // private static final org.slf4j.Logger log =
    // LoggerFactory.getLogger(hiveConnect.class);
    private final static Logger logger = LoggerFactory.getLogger(HiveJDBCConnect.class);
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static String url = "jdbc:hive2://10.110.19.9:10000/panda_realtime";
    private static String user = "bigdata";
    private static String password = "cHjhAFeSDpBMrkxyOPzp";
    private static String sql = "";
    private static ResultSet res;
    private static int tryTimes=0;
    private static Connection conn =null;
    private static Statement stmt=null;
    private ResultSet result=null;

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

    public void write(String path, List<List<String>> list) {
        path = (path.endsWith("/")) ? path : (path + "/");
        Configuration conf = new Configuration();
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("core-site.xml"));
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
        FileSystem fs = null;
        CompressionOutputStream cmpOut = null;
        FSDataOutputStream hdfsOutStream = null;

        try {
            fs = FileSystem.get(conf);
            CompressionCodecFactory e = new CompressionCodecFactory(conf);
            CompressionCodec codec = e.getCodecByName("DEFLATE");
            Compressor compressor = CodecPool.getCompressor(codec, conf);
            String filepath = path + System.currentTimeMillis() + "_" + Math.random() + ".deflate";
            if(fs.exists(new Path(filepath))) {
                hdfsOutStream = fs.append(new Path(filepath));
            } else {
                hdfsOutStream = fs.create(new Path(filepath));
            }

            cmpOut = codec.createOutputStream(hdfsOutStream, compressor);
            Iterator var12 = list.iterator();

            while(var12.hasNext()) {
                List listSub = (List)var12.next();
                byte[] line = this.consumeRecordToHive(listSub);
                if(line != null) {
                    cmpOut.write(line);
                }
            }
        } catch (IOException var30) {
            var30.printStackTrace();
        } finally {
            try {
                cmpOut.close();
            } catch (IOException var29) {
                var29.printStackTrace();
            }

            try {
                hdfsOutStream.close();
            } catch (IOException var28) {
                var28.printStackTrace();
            }

            try {
                fs.close();
            } catch (IOException var27) {
                var27.printStackTrace();
            }

        }

    }

    public String consumeHiveRecord(String column, int type) {
        String result = null;
        switch(type) {
            case -5:
                result = column != null && !column.isEmpty()?column:"0";
                break;
            case 4:
                result = column != null && !column.isEmpty()?column:"0";
                break;
            case 6:
                result = column != null && !column.isEmpty()?column:"0";
                break;
            case 8:
                result = column != null && !column.isEmpty()?column:"0";
                break;
            case 12:
                result = column != null && !column.isEmpty()?column:"";
                break;
            default:
                result = column != null && !column.isEmpty()?column:"";
        }

        return result;
    }

    private byte[] consumeRecordToHive(List<String> listSub) {
        String sep = ",";

        try {
            sep = new String(new byte[]{(byte)1}, "UTF-8");
            String e = null;
            int num = 0;

            for(Iterator var6 = listSub.iterator(); var6.hasNext(); ++num) {
                String str = (String)var6.next();
                if(str == null || str.equals("\\N")) {
                    str = "0";
                }

                if(num == 0) {
                    e = str;
                } else {
                    e = e + sep + str;
                }
            }

            return (e + "\n").getBytes("UTF-8");
        } catch (UnsupportedEncodingException var7) {
            return null;
        }
    }
    public void write2(String path, List<String> list,String job,String curMinute) {
        path = (path.endsWith("/")) ? path : (path + "/");
        Configuration conf = new Configuration();
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("core-site.xml"));
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        org.apache.hadoop.fs.FileSystem fs;
        try {
            fs = org.apache.hadoop.fs.FileSystem.get(conf);
            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            FSDataOutputStream hdfsOutStream = null;
            CompressionCodec codec = factory.getCodecByName("DEFLATE");
            Compressor compressor = CodecPool.getCompressor(codec, conf);
            CompressionOutputStream cmpOut = null;
            String filepath = path + System.currentTimeMillis() + "_" + job+  curMinute + ".deflate";
            if (fs.exists(new Path(filepath))) {
                hdfsOutStream = fs.append(new Path(filepath));
            } else {
                hdfsOutStream = fs.create(new Path(filepath));
            }

            cmpOut = codec.createOutputStream(hdfsOutStream, compressor);
            for (String subStr : list) {
                byte[] line = (subStr + "\n").getBytes("UTF-8");
                if (line == null)
                    continue;
                cmpOut.write(line);
            }
            cmpOut.finish();
            cmpOut.close();
            hdfsOutStream.close();
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public void write2(String path, Set<DetailAnchor> set) {
        path = (path.endsWith("/")) ? path : (path + "/");
        Configuration conf = new Configuration();
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("core-site.xml"));
        conf.addResource(HiveJDBCConnect.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        org.apache.hadoop.fs.FileSystem fs;
        try {
            fs = org.apache.hadoop.fs.FileSystem.get(conf);
            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            FSDataOutputStream hdfsOutStream = null;
            CompressionCodec codec = factory.getCodecByName("DEFLATE");
            Compressor compressor = CodecPool.getCompressor(codec, conf);
            CompressionOutputStream cmpOut = null;
            String filepath = path + System.currentTimeMillis() + "_" + Math.random() + ".deflate";
            if (fs.exists(new Path(filepath))) {
                hdfsOutStream = fs.append(new Path(filepath));
            } else {
                hdfsOutStream = fs.create(new Path(filepath));
            }

            cmpOut = codec.createOutputStream(hdfsOutStream, compressor);
            for (DetailAnchor detailAnchor : set) {
                byte[] line = (detailAnchor.toString() + "\n").getBytes("UTF-8");
                if (line == null)
                    continue;
                cmpOut.write(line);
            }
            cmpOut.finish();
            cmpOut.close();
            hdfsOutStream.close();
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public  ResultSet excutesql(String sql) {
        try {
            if (conn == null) {
                conn = getConn();
            }
            stmt = conn.createStatement();
//			new GetLogThread().start();
            result = stmt.executeQuery(sql);


        } catch (ClassNotFoundException | SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.warn(e.toString());
            if (e.toString().contains("return code 1") && tryTimes < 5) {
                tryTimes++;
                int waitingTime = tryTimes * 120000;
                logger.warn("maybe timesout; 30 second later retry " + tryTimes + "times;");
                try {
                    Thread.sleep(waitingTime);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                return excutesql(sql);
            }
            logger.error("try 3 times, exit");
        }
        return result;
    }
}
