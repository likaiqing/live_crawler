package com.pandatv.common;

import com.pandatv.pojo.*;
import com.pandatv.tools.DateTools;
import com.pandatv.tools.HiveJDBCConnect;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpHost;
import sun.misc.BASE64Encoder;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2016/11/7.
 */
public abstract class PandaProcessor implements PageProcessor {
    protected static Set<String> detailAnchors = new HashSet<>();
    protected static Set<DetailAnchor> detailAnchorObjs = new HashSet<>();
    protected static Set<GiftInfo> douyuGiftObjs = new HashSet<>();
    protected static Set<String> douyuGifts = new HashSet<>();
    protected static Set<String> anchors = new HashSet<>();
    protected static Set<Anchor> anchorObjs = new HashSet<>();
    public static String job;
    public static StringBuffer failedUrl = new StringBuffer("failedUrl:");
    public static StringBuffer timeOutUrl = new StringBuffer("timeOutUrl:");
    public static String from = DateTools.getCurDate();
    public static String curMinute = DateTools.getCurMinute();
    public static String date;
    public static String hour;
    public static long s = System.currentTimeMillis();
    public static HiveJDBCConnect hive = new HiveJDBCConnect();
    public static String mailHours = "";
    public static String douyuGiftHours = "02,06,09,12,15,18,22";

    private static String randomStr = RandomStringUtils.random(10, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'});
    private static String randomTime = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date());
    public static String mailMinuteStr = new SimpleDateFormat("mm").format(new Date());

    //twitch相关
    protected static Set<TwitchCategory> twitchCatObjes = new HashSet<>();
    protected static Set<String> twitchCatStrs = new HashSet<>();
    protected static Set<TwitchChannel> twitchListObjes = new HashSet<>();
    protected static Set<String> twitchListStrs = new HashSet<>();
    protected static int thread = 1;
    private static List<String> userAgents = new ArrayList<>();
    private static String userAgent = "";

    static {
        int r = (int) Math.random() * 10;
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0");
        userAgents.add("Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.2561.400 QQBrowser/9.6.10822.400");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.108 Safari/537.36 2345Explorer/8.5.0.15179");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393");
        userAgent = userAgents.get(r);
    }

    protected Site site = Site.me()
            .setSleepTime(10)
            .setTimeOut(5 * 1000)
            .setUseGzip(true)
            .setRetryTimes(5)
            .setCharset("UTF-8")
            .setRetrySleepTime(1)
            .setCycleRetryTimes(Const.CYCLERETRYTIMES)
            .setHttpProxy(new HttpHost(Const.ABUYUNPHOST, Const.ABUYUNPORT))
            .addHeader("Proxy-Switch-Ip", "yes")
            .setUserAgent(userAgent)
            .addHeader("Proxy-Authorization", "Basic " + (new BASE64Encoder()).encode((Const.GENERATORKEY + ":" + Const.GENERATORPASS).getBytes()));

    public static String getRandomStr() {
        return randomTime + "-" + randomStr;
    }
}
