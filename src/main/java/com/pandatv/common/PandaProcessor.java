package com.pandatv.common;

import com.pandatv.tools.DateTools;
import com.pandatv.tools.HiveJDBCConnect;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpHost;
import sun.misc.BASE64Encoder;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/7.
 */
public abstract class PandaProcessor implements PageProcessor {
    protected static List<String> detailAnchors = new ArrayList<>();
    protected static StringBuffer failedUrl = new StringBuffer("failedUrl:");
    protected static StringBuffer timeOutUrl = new StringBuffer("timeOutUrl:");
    protected static String curMinute = DateTools.getCurMinute();
    protected static long s = System.currentTimeMillis();
    protected static HiveJDBCConnect hive = new HiveJDBCConnect();

    private static String randomStr = RandomStringUtils.random(10, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'});
    private static String randomTime = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date());

    protected Site site = Site.me()
            .setSleepTime(500)
            .setUseGzip(true)
            .setRetryTimes(5)
            .setCharset("UTF-8")
            .setRetrySleepTime(1)
            .setCycleRetryTimes(Const.CYCLERETRYTIMES)
            .setHttpProxy(new HttpHost(Const.ABUYUNPHOST,Const.ABUYUNPORT))
            .addHeader("Proxy-Switch-Ip","yes")
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:45.0) Gecko/20100101 Firefox/45.0")
            .addHeader("Proxy-Authorization","Basic "+ (new BASE64Encoder()).encode((Const.GENERATORKEY+":"+Const.GENERATORPASS).getBytes()));

    public static String getRandomStr() {
        return randomStr + "-" + randomTime;
    }
}
