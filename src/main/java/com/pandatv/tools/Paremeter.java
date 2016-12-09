package com.pandatv.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by likaiqing on 2016/12/8.
 */
public class Paremeter {
    private static final String propertiesFileName = "config.properties";
    private final Logger logger = LoggerFactory.getLogger(Paremeter.class);

    public String partitionSpec = "";
    public String key = "";

    public String mysqlcolumns = "";
    public String mysqltable = "";

    public String propertiesFile = "";
    public String downloadPath = "";
    public String downloadSep = "";
    public String uploadPath = "";
    public String uploadSep = "";
    public String par = "";
    public String hivesql="";
    public Boolean overwriteMysql=false;
    public String overwriteColumns="";
    public String hdfsFilePath="";
    public String DBURL = "";
    public String name = "";
    public String DBUSER = "";
    public String DBPASS = "";

    public String DBURL2 = "";
    public String DBUSER2 = "";
    public String DBPASS2 = "";
    public Boolean isPartition=true;
    public Boolean importOverwrite=false;
    public String queue="";

    public String hivetable="";
    public String sendTo="";
    public String sendCC="";
    public String mailname="";
    public String hivedatabase="panda_realtime";


    Paremeter(String sql) {
        Properties pro=getDefaultParemerter();
        getProperties(pro);


        if(!sql.equals(""))convertSQL(sql);
    }
    public void getParemeter(String path){
        File file=new File(path);
        if(!file.isFile()&&!file.isDirectory()){
            getProperties(getParemerter(path));
        }else{
            getProperties(getParemerterFromFile(file));
        }
    }
    private void convertSQL(String sql){
        hivesql = sql.substring(sql.toLowerCase().indexOf("select"), sql.indexOf(";") + 1).replace(";", "");

        key = sql.substring(0, sql.toLowerCase().indexOf("select"));

        if (key.toLowerCase().indexOf("insert") >= 0) {
            if(key.toLowerCase().indexOf("overwrite")>0){
                overwriteMysql=true;
            }
            loadInsert();
        } else if (key.toLowerCase().indexOf("download") >= 0) {
            loadDownload();
        } else if (key.toLowerCase().indexOf("import") >= 0) {
            loadImport();
        }  else if (key.toLowerCase().indexOf("send") >= 0) {

            loadSend();
        }
    }

    private void loadSend() {
        mailname=get(key);
        String subkey=key.substring(key.indexOf(")")+1,key.length());

        sendTo=get(subkey);
        if(subkey.toLowerCase().contains("cc")){
            String subsubkey=subkey.substring(subkey.indexOf(")")+1,subkey.length());
            sendCC=get(subsubkey);
        }

    }
    private String get(String subkey){
        int startindex = 0;
        int endindex = 0;
        String result="";
        if (subkey.indexOf("(") >=0) {
            startindex = subkey.indexOf("(") + 1;
            endindex = subkey.indexOf(")", startindex);
            result = subkey.substring(startindex, endindex);
            return result;
        }else{
            return null;
        }

    }
    private void loadInsert() {
        int startindex = 0;
        int endindex = 0;
        String subkey="";
        if(overwriteMysql){
            startindex=key.indexOf("(")+1;
            endindex=key.indexOf(")");
            if(startindex>0&&endindex>0&&!key.substring(endindex).replace(" ", "").equals("")){
                overwriteColumns=key.substring(startindex,endindex);
                subkey=key.substring(endindex+1);
            }else{
                logger.error("insert overwrite must with (overwriteColumns");
                System.exit(0);
            }


        }else{
            subkey=key;
        }

        if (subkey.indexOf("(") > 0) {
            startindex = subkey.indexOf("(") + 1;
            endindex = subkey.indexOf(")", startindex);
            if (startindex < 0) {
                logger.info("miss ')',please check;");
                System.exit(0);
            }
            if (endindex < 0) {
                logger.info("miss ')',please check;");
                System.exit(0);
            }
            mysqlcolumns = subkey.substring(startindex, endindex);
        }
        if(subkey.toLowerCase().indexOf("into")>0){
            startindex = subkey.toLowerCase().indexOf("into") + 4;
        }else{
            startindex=0;
        }

        endindex = (subkey.indexOf("(") > 0) ? subkey.indexOf("(") : subkey.length();
        mysqltable = subkey.substring(startindex, endindex).replace(" ", "");
    }

    private void loadDownload() {
        int startindex = 0;
        int endindex = 0;
        startindex = key.toLowerCase().indexOf("download") + 8;
        endindex = key.indexOf("-") > 0 ? key.indexOf("-") : key.length();
        downloadPath = key.substring(startindex, endindex).replace(" ", "").replace("'", "");
    }

    private void loadImport() {
        int startindex = 0;
        int endindex = 0;
        if(key.contains("overwrite")){
            importOverwrite=true;
            startindex = key.toLowerCase().replace(" ", "").indexOf("importoverwrite")+15;

        }else if(key.contains("into")){
            startindex = key.toLowerCase().replace(" ", "").indexOf("importinto")+10;
        }else{
            logger.error("sql error;miss into or overwrite");
        }


        endindex = key.toLowerCase().replace(" ", "").indexOf("(") > 0 ? key.toLowerCase().replace(" ", "").indexOf("(") : key.toLowerCase().replace(" ", "").length();
        hivetable = key.toLowerCase().replace(" ", "").substring(startindex, endindex).replace(" ", "").replace("'", "");
        if(hivetable.contains(".")){
            hivedatabase=hivetable.substring(0, hivetable.indexOf("."));
            hivetable=hivetable.substring( hivetable.indexOf(".")+1,hivetable.length());

        }

        startindex=key.toLowerCase().indexOf("(")+1;
        if(startindex<=0) {
            isPartition=false;

            return;
        }
        endindex=key.toLowerCase().indexOf("=",startindex);
        partitionSpec=key.toLowerCase().substring(startindex, endindex);
        par=key.toLowerCase().substring(endindex+1, key.toLowerCase().indexOf(")"));


    }

    private void getProperties(Properties pro) {

        hdfsFilePath = pro.getProperty("filepath", "/bigdata/hive/panda_realtime/");
        DBURL = pro.getProperty("DBURL");
        name = pro.getProperty("name");
        DBUSER = pro.getProperty("DBUSER");
        DBPASS = pro.getProperty("DBPASS");
        DBURL2 = pro.getProperty("DBURL2");
        DBUSER2 = pro.containsKey(DBUSER2)?pro.getProperty("DBUSER2"):pro.getProperty("DBUSER");
        DBPASS2 = pro.containsKey(DBPASS2)?pro.getProperty("DBPASS2"):pro.getProperty("DBPASS");
        queue= pro.getProperty("queue","default");


    }


    private Properties getDefaultParemerter(){
        return getParemerter(propertiesFileName);
    }


    private Properties getParemerter(String config){
        Properties pro = new Properties();
        try {
            pro.load(Paremeter.class.getClassLoader().getResourceAsStream(config));
            return pro;
        } catch (IOException e2) {
            return null;
        }
    }

    private Properties getParemerterFromFile(File file) {
        Properties pro = new Properties();
        try {
            InputStreamReader read = new InputStreamReader(new FileInputStream(file));
            pro.load(read);
            return pro;
        } catch (IOException e) {
            logger.info("can not find properties file; load default;");
            return null;
        }




    }
}
