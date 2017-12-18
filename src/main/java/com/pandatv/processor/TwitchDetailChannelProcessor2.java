package com.pandatv.processor;

import com.google.common.base.Splitter;
import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.TwitchDetailChannel;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.scheduler.PriorityScheduler;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by likaiqing on 2017/3/21.
 */
public class TwitchDetailChannelProcessor2 extends PandaProcessor {
    private static int cnt;
    private static final Logger logger = LoggerFactory.getLogger(TwitchDetailChannelProcessor2.class);
    private static List<String> pageListurls = new ArrayList<>();
    private static Map<String, TwitchDetailChannel> map = new HashMap<>();

    private static String urlPre = "https://api.twitch.tv/kraken/streams?broadcaster_language=&on_site=1&game=";//limit=20&offset=0&
    private static String teamNamePre = "https://api.twitch.tv/api/channels/";
    private static String teamNameSuf = "/ember?on_site=1";
    private static String videosPre = "https://api.twitch.tv/kraken/channels/";
    private static String videosSuf = "/videos?limit=60&offset=0&broadcast_type=archive%2Cupload%2Chighlight&on_site=1";
    private static String followingPre = "https://api.twitch.tv/kraken/users/";
    private static String followingSuf = "/follows/channels?offset=0&on_site=1&on_site=1";
    private static int exCnt;

    @Override
    public void process(Page page) {

    }

    @Override
    public Site getSite() {
        site.addHeader("client-id", "jzkbprff40iqj646a697cyrvl0zt2m6");
        if (!useProxy) {
            site.setHttpProxy(null);
        }
        return site;
    }

    public static void main(String[] args) {
        job = args[0];
        date = args[1];
        hour = args[2];
        int threads = 22;
        if (args.length == 4 && args[3].matches("\\d+")) {
            try {
                threads = Integer.parseInt(args[3]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (args.length == 4 && args[3].equals("false")) {
            useProxy = false;
        }
        if (args.length == 5) {
            try {
                threads = Integer.parseInt(args[3]);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (args[4].equals("false")){
                useProxy = false;
            }
        }
//        Const.GENERATORKEY = "H7ABSOS1FI3M9I4P";
//        Const.GENERATORPASS = "97CCB7E9284ACAF0";
//        Const.GENERATORKEY = "panda";
//        Const.GENERATORPASS = "pandatv";
        String hivePath = Const.COMPETITORDIR + "crawler_twitch_detail_channel/" + date;
        String firstUrl = "https://api.twitch.tv/kraken/games/top?limit=40&on_site=1";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("start:" + format.format(new Date()));
        long start = System.currentTimeMillis();
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook()));
        Spider.create(new TwitchDetailChannelProcessor2()).thread(threads).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).setScheduler(new PriorityScheduler()).run();
        System.out.println("end:" + format.format(new Date()));
        System.out.println(cnt);
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000 + 1;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / (0 == secs ? 1 : secs)) + ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
        executeMapResults();

    }

    private static void executeMapResults() {
        for (Map.Entry<String, TwitchDetailChannel> entry : map.entrySet()) {
            TwitchDetailChannel tdc = entry.getValue();
            if (null != tdc.getTeamName() && null != tdc.getVideos() && null != tdc.getFollowing()) {
                resultSetStr.add(entry.getValue().toString());
            }
        }
        logger.info("resultSetStr.size:" + resultSetStr.size());
        String dirFile = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append(job).append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile, resultSetStr);
    }

    private static class ShutDownHook implements Runnable {

        @Override
        public void run() {
            logger.info("writeSuccess:" + writeSuccess);
            if (!writeSuccess) {
                executeMapResults();
            }
        }
    }
}
