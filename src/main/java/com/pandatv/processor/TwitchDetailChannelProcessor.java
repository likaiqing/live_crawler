package com.pandatv.processor;

import com.google.common.base.Splitter;
import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.TwitchDetailChannel;
import com.pandatv.tools.HttpUtil;
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
public class TwitchDetailChannelProcessor extends PandaProcessor {
    private static int cnt;
    private static final Logger logger = LoggerFactory.getLogger(TwitchDetailChannelProcessor.class);
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
        requests++;
        String json = page.getJson().get();
        String curUrl = page.getUrl().get();
        try {
            if (curUrl.startsWith("https://api.twitch.tv/kraken/games/top?")) {
                int total = JsonPath.read(json, "$._total");
                String nextUrl = JsonPath.read(json, "$._links.next");
                JSONArray arr = JsonPath.read(json, "$.top");
                for (int i = 0; i < arr.size(); i++) {
                    String cate = arr.get(i).toString();
                    String name = JsonPath.read(cate, "$.game.name");
                    String encode = URLEncoder.encode(name, "utf-8").replace("%20", "+");
//                    pageListurls.add(urlPre + encode + "&limit=20&offset=0");
                    page.addTargetRequest(new Request(urlPre + encode + "&limit=20&offset=0").setPriority(1));
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
                    TwitchDetailChannel channel = new TwitchDetailChannel();
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
                    page.addTargetRequest(teamNamePre + name + teamNameSuf);
                    page.addTargetRequest(videosPre + name + videosSuf);
                    page.addTargetRequest(followingPre + name + followingSuf);
                    map.put(name, channel);
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
            } else if (curUrl.contains(teamNameSuf)) {
                String nickName = JsonPath.read(json, "$.name").toString();
                String teamName = JsonPath.read(json, "$.primary_team_display_name");
                TwitchDetailChannel twitchDetailChannel = map.get(nickName);
                twitchDetailChannel.setTeamName(null == teamName ? "" : teamName);
                synchronized (this) {
                    cnt++;
                }
            } else if (curUrl.contains(videosSuf)) {
                int videos = JsonPath.read(json, "$._total");
                String selfUrl = JsonPath.read(json, "$._links.self");
                String nickName = selfUrl.substring(selfUrl.indexOf("channels/") + 9, curUrl.indexOf("/videos?"));
                TwitchDetailChannel twitchDetailChannel = map.get(nickName);
                twitchDetailChannel.setVideos(videos);
                synchronized (this) {
                    cnt++;
                }
            } else if (curUrl.contains(followingSuf)) {
                int following = JsonPath.read(json, "$._total");
                String nickName = curUrl.replace(followingPre, "").replace(followingSuf, "");
                TwitchDetailChannel twitchDetailChannel = map.get(nickName);
                twitchDetailChannel.setFollowing(following);
                synchronized (this) {
                    cnt++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace();
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (++exCnt % 1000 == 0) {
                MailTools.sendAlarmmail("twitchdetailchannel 异常请求个数过多", "url: " + failedUrl.toString());
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
        String hivePath = Const.COMPETITORDIR + "crawler_twitch_detail_channel/" + date;
        String firstUrl = "https://api.twitch.tv/kraken/games/top?limit=40&on_site=1";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("start:" + format.format(new Date()));
        long start = System.currentTimeMillis();
        Spider.create(new TwitchDetailChannelProcessor()).thread(20).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).setScheduler(new PriorityScheduler()).run();
        System.out.println("end:" + format.format(new Date()));
        System.out.println(cnt);
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs)+ ",异常个数:" + exCnt + ",fialedurl:" + failedUrl.toString());
        for (Map.Entry<String, TwitchDetailChannel> entry : map.entrySet()) {
            TwitchDetailChannel tdc = entry.getValue();
            if (null != tdc.getTeamName() && null != tdc.getVideos() && null != tdc.getFollowing()) {
//                twitchListStrs.add(entry.getValue().toString());
//                HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN)
//                        .append(Const.TWITCHDETAILCHAEVENT)
//                        .append("&par_d=").append(date)
//                        .append("&id=").append(tdc.getId())
//                        .append("&nick_nm=").append(tdc.getNickName())
//                        .append("&dis_nm=").append(tdc.getDisplayName())
//                        .append("&tt=").append(tdc.getTitle())
//                        .append("&plat=").append(tdc.getPlat())
//                        .append("&game=").append(tdc.getGame())
//                        .append("&broa_lan=").append(tdc.getBroadcasterLan())
//                        .append("&lan=").append(tdc.getLanguage())
//                        .append("&reg_time=").append(tdc.getRegisterTime())
//                        .append("&url=").append(tdc.getUrl())
//                        .append("&vies=").append(tdc.getViewers())
//                        .append("&vie_tol=").append(tdc.getViewsTol())
//                        .append("&fols=").append(tdc.getFollowers())
//                        .append("&cur_u=").append(tdc.getCurUrl())
//                        .append("&task=").append(job)
//                        .append("&c_time=").append(createTimeFormat.format(new Date()))
//                        .append("&t_ran=").append(getRandomStr())
//                        .append("&team_nm=").append(tdc.getTeamName())
//                        .append("&viedos=").append(tdc.getVideos())
//                        .append("&fol_ing=").append(tdc.getFollowing()).toString());

                try {
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN)
                                    .append(Const.TWITCHDETAILCHAEVENT)
                                    .append("&par_d=").append(date).append(tdc.toString()).toString());
                        }
                    }).start();
                    Thread.sleep(10);
                }catch (Exception e){

                }

            }
        }
//        CommonTools.writeAndMail(hivePath, Const.TWITCHDETAILCHANNELFINISH, twitchListStrs);
    }
}
