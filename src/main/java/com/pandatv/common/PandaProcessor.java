package com.pandatv.common;

import com.pandatv.pojo.*;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.DateTools;
import com.pandatv.tools.HiveJDBCConnect;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2016/11/7.
 */
public abstract class PandaProcessor implements PageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PandaProcessor.class);
    protected static Set<String> detailAnchors = new HashSet<>();
    protected static Set<DetailAnchor> detailAnchorObjs = new HashSet<>();
    protected static Set<GiftInfo> douyuGiftObjs = new HashSet<>();
    protected static Set<String> douyuGifts = new HashSet<>();
    protected static Set<String> anchors = new HashSet<>();
    protected static Set<Anchor> anchorObjs = new HashSet<>();
    protected static Set<String> resultSetStr = new HashSet<>();
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
    public static boolean writeSuccess = false;
    protected static boolean useProxy = true;

    protected static String randomStr = RandomStringUtils.random(10, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'});
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
    private static List<String[]> httpProxyList = new ArrayList<>();

    protected static HttpHost httpHost = new HttpHost("180.97.220.231", 9997);

    static {
        int r = (int) Math.random() * 20;

        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36 LBBROWSER");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2626.106 Safari/537.36 Yunhai Browser");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 qblink tgp_daemon.exe QBCore/3.43.549.400 QQBrowser/9.0.2524.400");
        userAgents.add("Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 UBrowser/6.1.3228.1 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.3226.400 QQBrowser/9.6.11682.400");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 UBrowser/6.1.3228.1 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36");
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko");
        userAgent = userAgents.get(r);
//        encoder = Base64.getEncoder();
        httpProxyList.add(new String[]{"222.186.169.4","9997","panda","pandatvpassw0rd"});
        httpProxyList.add(new String[]{"222.186.169.13","9997","panda","pandatvpassw0rd"});
        httpProxyList.add(new String[]{"222.186.169.66","9997","panda","pandatvpassw0rd"});
        httpProxyList.add(new String[]{"222.186.169.76","9997","panda","pandatvpassw0rd"});
        httpProxyList.add(new String[]{"222.186.42.79","9997","panda","pandatvpassw0rd"});
        httpProxyList.add(new String[]{"180.97.220.210","9997","panda","pandatvpassw0rd"});
        httpProxyList.add(new String[]{"180.97.220.231","9997","panda","pandatvpassw0rd"});
    }

    protected Site site = Site.me()
            .setSleepTime(10)
            .setTimeOut(5 * 1000)
            .setUseGzip(true)
            .setRetryTimes(5)
            .setCharset("UTF-8")
            .setRetrySleepTime(1)
            .setCycleRetryTimes(Const.CYCLERETRYTIMES)
//            .setHttpProxy(new HttpHost(Const.ABUYUNPHOST, Const.ABUYUNPORT))
//            .setHttpProxyPool(httpProxyList)
            .addHeader("Proxy-Switch-Ip", "yes")
            .setUserAgent(userAgent)
            .addHeader("Proxy-Authorization", "Basic " + (new BASE64Encoder()).encode((Const.GENERATORKEY + ":" + Const.GENERATORPASS).getBytes()));

    public static String getRandomStr() {
        return randomTime + "-" + randomStr;
    }

    protected static void executeResults() {
        logger.info("executeResults resultSetStr.size:"+resultSetStr.size());
        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile, resultSetStr);
    }

    public static class ShutDownHook implements Runnable {
        @Override
        public void run() {
            logger.info("writeSuccess:"+writeSuccess);
            if (!writeSuccess) {
                executeResults();
            }
        }
    }

    @Override
    public Site getSite() {
        if (!useProxy) {
            site.setHttpProxy(null);
        }else {
            //需要修改Const.GENERATORKEY="panda";Const.GENERATORPASS="pandatvpassw0rd";
            int i = (int) ((Math.random()) * 7);
            HttpHost httpHost = new HttpHost("180.97.220.231", 9997);
            try {
                httpHost = new HttpHost(httpProxyList.get(i)[0],Integer.parseInt(httpProxyList.get(i)[1]));
            }catch (Exception e){
                e.printStackTrace();
            }
            site.setHttpProxy(httpHost);
        }

        return site;
//        return site;
    }

    protected static void initParam(String[] args){
        if (args.length == 4 && args[3].matches("\\d+")) {
            try {
                thread = Integer.parseInt(args[3]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (args.length == 4 && args[3].equals("false")) {
            useProxy = false;
        }
        if (args.length == 5) {
            try {
                thread = Integer.parseInt(args[3]);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (args[4].equals("false")) {
                useProxy = false;
            }
        }
    }

}
