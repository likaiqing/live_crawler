package com.pandatv.processor;

import com.google.common.base.Splitter;
import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.TwitchCategory;
import com.pandatv.pojo.TwitchChannel;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.monitor.SpiderMonitor;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.scheduler.PriorityScheduler;

import javax.management.JMException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by likaiqing on 2017/3/21.
 */
public class TwitchCateAndListProcessor extends PandaProcessor {
    private static int cnt;
    private static final Logger logger = LoggerFactory.getLogger(TwitchCateAndListProcessor.class);
    private static int exCnt;
//    private static List<String> pageListurls = new ArrayList<>();
    /**
     * 列表页url前缀
     */
    private static String urlPre = "https://api.twitch.tv/kraken/streams?broadcaster_language=&on_site=1&game=";//limit=20&offset=0&

    @Override
    public void process(Page page) {
        requests++;
        String json = page.getJson().get();
        String curUrl = page.getUrl().get();
//        Map<String, String> splitMap = Splitter.on("&").withKeyValueSeparator("=").split(curUrl.substring(curUrl.indexOf("=") + 1));
        try {
            if (curUrl.startsWith("https://api.twitch.tv/kraken/games/top?")) {
                int total = JsonPath.read(json, "$._total");
                String nextUrl = JsonPath.read(json, "$._links.next");
                JSONArray arr = JsonPath.read(json, "$.top");
                for (int i = 0; i < arr.size(); i++) {
                    String cate = arr.get(i).toString();
                    String name = JsonPath.read(cate, "$.game.name");
                    int id = JsonPath.read(cate, "$.game._id");
                    int giantBombId = JsonPath.read(cate, "$.game.giantbomb_id");
                    int viewers = JsonPath.read(cate, "$.viewers");
                    int channels = JsonPath.read(cate, "$.channels");
//                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.TWITCHCATEEVENT)
//                            .append("&par_d=").append(date)
//                            .append("&id=").append(id)
//                            .append("&g_b_id=").append(giantBombId)
//                            .append("&plat=").append(Const.TWITCH)
//                            .append("&nm=").append(name)
//                            .append("&vies=").append(viewers)
//                            .append("&chas=").append(channels)
//                            .append("&cur_u=").append(curUrl)
//                            .append("&next_u=").append(nextUrl)
//                            .append("&task=").append(job)
//                            .append("&c_time=").append(createTimeFormat.format(new Date()))
//                            .append("&t_ran=").append(getRandomStr()).toString());
                    TwitchCategory twitchCategory = new TwitchCategory();
                    twitchCategory.setId(id);
                    twitchCategory.setGiantBombId(giantBombId);
                    twitchCategory.setPlat(Const.TWITCH);
                    twitchCategory.setName(name);
                    twitchCategory.setViewers(viewers);
                    twitchCategory.setChannels(channels);
                    twitchCategory.setCurUrl(curUrl);
                    twitchCategory.setNextUrl(nextUrl);
                    twitchCategory.setTask(job);
//                    try {
//                        HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.TWITCHCATEEVENT)
//                                .append("&par_d=").append(date).append(twitchCategory.toString()).toString());
//                        Thread.sleep(10);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
                    twitchCatObjes.add(twitchCategory);
                    String encode = URLEncoder.encode(name, "utf-8").replace("%20", "+");
                    page.addTargetRequest(new Request(urlPre + encode + "&limit=20&offset=0").setPriority(1));
//                    pageListurls.add(urlPre + encode + "&limit=20&offset=0");
                }
                Map<String, String> splitMap = Splitter.on("&").withKeyValueSeparator("=").split(curUrl.substring(curUrl.indexOf("?") + 1));
                int offSet = Integer.parseInt(null == splitMap.get("offset") ? "0" : splitMap.get("offset"));
                if (offSet + 40 < total) {
                    page.addTargetRequest(new Request(nextUrl).setPriority(4));
                }
//                else {
//                    page.addTargetRequests(pageListurls);
//                }
                synchronized (this) {
                    cnt++;
                }
            } else if (curUrl.startsWith(urlPre)) {
                JSONArray arr = JsonPath.read(json, "$.streams");
                int total = JsonPath.read(json, "$._total");
                for (int i = 0; i < arr.size(); i++) {
                    String str = arr.get(i).toString();
                    TwitchChannel channel = new TwitchChannel();
                    int id = JsonPath.read(str, "$.channel._id");
                    String name = JsonPath.read(str, "$.channel.name");
                    String displayName = JsonPath.read(str, "$.channel.display_name");
                    String title = JsonPath.read(str, "$.channel.status");
                    String game = JsonPath.read(str, "$.channel.game");
                    String broadcasterLan = JsonPath.read(str, "$.channel.broadcaster_language");
                    String language = JsonPath.read(str, "$.channel.language");
                    String registerTime = JsonPath.read(str, "$.channel.created_at");
                    String url = JsonPath.read(str, "$.channel.url");
                    int viewers = JsonPath.read(str, "$.viewers");
                    int viewsTol = JsonPath.read(str, "$.channel.views");
                    int followers = JsonPath.read(str, "$.channel.followers");
//                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.TWITCHCHAEVENT)
//                            .append("&par_d=").append(date)
//                            .append("&id=").append(id)
//                            .append("&nick_nm=").append(name)
//                            .append("&dis_nm=").append(displayName)
//                            .append("&tt=").append(title)
//                            .append("&plat=").append(Const.TWITCH)
//                            .append("&game=").append(game)
//                            .append("&broa_lan=").append(broadcasterLan)
//                            .append("&lan=").append(language)
//                            .append("&reg_time=").append(registerTime)
//                            .append("&url=").append(url)
//                            .append("&vies=").append(viewers)
//                            .append("&vie_tol=").append(viewsTol)
//                            .append("&fols=").append(followers)
//                            .append("&cur_u=").append(curUrl)
//                            .append("&task=").append(job)
//                            .append("&c_time=").append(createTimeFormat.format(new Date()))
//                            .append("&t_ran=").append(getRandomStr()).toString());
                    channel.setId(id);
                    channel.setNickName(name);
                    channel.setDisplayName(displayName);
                    channel.setTitle(title);
                    channel.setPlat(Const.TWITCH);
                    channel.setGame(game);
                    channel.setBroadcasterLan(broadcasterLan);
                    channel.setLanguage(language);
                    channel.setRegisterTime(registerTime);
                    channel.setUrl(url);
                    channel.setViewers(viewers);
                    channel.setViewsTol(viewsTol);
                    channel.setFollowers(followers);
                    channel.setCurUrl(curUrl);
                    channel.setTask(job);
//                    try {
//                        new Thread(new Runnable() {
//                            @Override
//                            public void run() {
//                                HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.TWITCHCHAEVENT)
//                                        .append("&par_d=").append(date).append(channel.toString()).toString());
//                            }
//                        }).start();
//                        Thread.sleep(5);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
                    twitchListObjes.add(channel);
                }
                int offSet = Integer.parseInt(curUrl.substring(curUrl.lastIndexOf("=") + 1));
                int newOffSet = 20;
                if (offSet != 0) {
                    newOffSet = 60 + offSet;
                }
                if (offSet + 60 < total) {
                    page.addTargetRequest(new Request(curUrl.substring(0, curUrl.indexOf("&limit")) + "&limit=60&offset=" + newOffSet).setPriority(4));
                }
                synchronized (this) {
                    cnt++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 100 == 0) {
                MailTools.sendAlarmmail("twitchcateandlist 异常请求个数过多", "url: " + failedUrl.toString());
//                System.exit(1);
            }
        }
    }

    @Override
    public Site getSite() {
        return this.site.addHeader("client-id", "jzkbprff40iqj646a697cyrvl0zt2m6");
    }

    public static void crawler(String[] args) {
        job = args[0];
        date = args[1];
        hour = args[2];
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        Const.GENERATORKEY = "H7ABSOS1FI3M9I4P";
        Const.GENERATORPASS = "97CCB7E9284ACAF0";
        String twitchCateHivePaht = Const.COMPETITORDIR + "crawler_twitch_category/" + date;
        String twitchListHivePaht = Const.COMPETITORDIR + "crawler_twitch_channel/" + date;
        //钩子
        Runtime.getRuntime().addShutdownHook(new Thread(new TwitchCateAndListShutDownHook()));

        String firstUrl = "https://api.twitch.tv/kraken/games/top?limit=40&on_site=1";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("start:" + format.format(new Date()));
        long start = System.currentTimeMillis();
        Spider spider = Spider.create(new TwitchCateAndListProcessor()).thread(18).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).setScheduler(new PriorityScheduler());//.run();
        try {
            SpiderMonitor.instance().register(spider);
        } catch (JMException e) {
            e.printStackTrace();
        }
        spider.start();

        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs)+ ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
//        System.out.println("end:" + format.format(new Date()));
//        System.out.println(cnt);

//        CommonTools.writeAndMail(twitchCateHivePaht, Const.TWITCHCATEFINISH, twitchCatStrs);
//        CommonTools.writeAndMail(twitchListHivePaht, Const.TWITCHLISTFINISH, twitchListStrs);

        executeMapResults();
    }

    private static void executeMapResults() {
        for (TwitchCategory obj : twitchCatObjes) {
            twitchCatStrs.add(obj.toString());
        }
        for (TwitchChannel obj : twitchListObjes) {
            twitchListStrs.add(obj.toString());
        }
        String dirFile1 = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append("twitchcategory").append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile1,twitchCatStrs);
        String dirFile2 = new StringBuffer(Const.CRAWLER_DATA_DIR).append(date).append("/").append(hour).append("/").append("twitchlist").append("_").append(date).append("_").append(hour).append(randomStr).toString();
        CommonTools.write2Local(dirFile2,twitchListStrs);
    }

    private static class TwitchCateAndListShutDownHook implements Runnable {
        @Override
        public void run() {
            logger.info("writeSuccess:"+writeSuccess);
            if (!writeSuccess){
                executeMapResults();
            }
        }
    }
}
