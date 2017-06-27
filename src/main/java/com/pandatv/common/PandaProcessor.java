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
    protected static SimpleDateFormat createTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected static int requests = 0;

    protected static Set<IndexRec> indexRecObjes = new HashSet<>();
//    public static Base64.Encoder encoder;

    static {
        int r = (int) Math.random() * 10;
        userAgents.add("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; NetworkBench/8.0.1.309-5653879-2740891)");
        userAgents.add("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; NetworkBench/8.0.1.309-5748187-2740891)");
        userAgents.add("CMDC M623C_LTE/V1 Linux/3.10.28 Android/5.1.1 Release/12.3.2015 Browser/AppleWebKit537.36 Mobile Safari/537.36 System/Android 5.1.1");
        userAgents.add("Android.Thunder.Mozilla/5.0 (Linux; Android 7.0; KNT-UL10 Build/HUAWEIKNT-UL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36");
        userAgents.add("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727)");
        userAgents.add("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 10.0; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; SLCC2; Media Center PC 6.0; Tablet PC 2.0)");
        userAgents.add("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; NetworkBench/8.0.1.309-3512246-2741620)");
        userAgents.add("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; QQDownload 718; SV1; .NET CLR 2.0.50727; {D9D54F49-E51C-445e-92F2-1EE3C2313240})");
        userAgents.add("HS-T967_TD/1.0 Linux/3.4.5 Android/4.1.2 Release/08.27.2013 Browser/AppleWebKit534.30 (KHTML, like Gecko) Mozilla/5.0 Mobile pandaclient");
        userAgents.add("K-Touch Tou ch3c/TBT595731_9291_V0001 Android/4.4.2 Release/20140401 Browser/AppleWebKit534.30 Profile/MIDP-2.0 Configuration/CLDC-1.1 pandaclient");
        userAgent = userAgents.get(r);
//        encoder = Base64.getEncoder();
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
