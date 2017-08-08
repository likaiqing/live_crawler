package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.HttpUtil;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by likaiqing on 2016/11/14.
 */
public class LongzhuAnchorProcessor extends PandaProcessor {
    private static String urlTmp = "http://api.plu.cn/tga/streams?max-results=18&sort-by=views&filter=0&game=0&callback=_callbacks_._36bxu1&start-index=";
    private static int pageCount = 18;
    private static int index = 0;
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final Logger logger = LoggerFactory.getLogger(LongzhuAnchorProcessor.class);
    private static final String firUrl = "http://api.plu.cn/tga/streams?max-results=18&sort-by=views&filter=0&game=0&callback=_callbacks_._36bxu1&start-index=0";
    private static int exCnt;

    @Override
    public void process(Page page) {
        requests++;
        String curUrl = page.getUrl().get();
        try {
            logger.info("process url:{}", curUrl);
            String json = page.getJson().toString();
            json = json.substring(20, json.lastIndexOf(")"));
            if (curUrl.equals(firUrl)) {
                Integer total = JsonPath.read(json, "$.data.totalItems");
                pageCount = Integer.parseInt(JsonPath.read(json, "$.data.limit").toString());
                while (index < total) {
                    index += pageCount;
                    page.addTargetRequest(urlTmp + (index));
                }
            } else {
                JSONArray items = JsonPath.read(json, "$.data.items");
                for (int i = 0; i < items.size(); i++) {
                    Anchor anchor = new Anchor();
                    String room = items.get(i).toString();
                    String rid = JsonPath.read(room, "$.channel.domain");
                    String name = JsonPath.read(room, "$.channel.name");
                    String title = JsonPath.read(room, "$.channel.status");
                    String category = JsonPath.read(room, "$.game[0].name");
                    String popularitiyStr = JsonPath.read(room, "$.viewers");
                    int popularitiyNum = Integer.parseInt(popularitiyStr);
//                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
//                            .append("&par_d=").append(date)
//                            .append("&rid=").append(rid)
//                            .append("&nm=").append(CommonTools.getFormatStr(name))
//                            .append("&tt=").append(CommonTools.getFormatStr(title))
//                            .append("&cate=").append(category)
//                            .append("&pop_s=").append(popularitiyStr)
//                            .append("&pop_n=").append(popularitiyNum)
//                            .append("&task=").append(job)
//                            .append("&plat=").append(Const.LONGZHU)
//                            .append("&url_c=").append(Const.GAMEALL)
//                            .append("&c_time=").append(createTimeFormat.format(new Date()))
//                            .append("&url=").append(curUrl)
//                            .append("&t_ran=").append(PandaProcessor.getRandomStr()).toString());
                    anchor.setRid(rid);
                    anchor.setName(name);
                    anchor.setTitle(title);
                    anchor.setCategory(category);
                    anchor.setPopularityStr(popularitiyStr);
                    anchor.setPopularityNum(popularitiyNum);
                    anchor.setJob(job);
                    anchor.setPlat(Const.LONGZHU);
                    anchor.setGame(Const.GAMEALL);
                    anchor.setUrl(curUrl);
                    HttpUtil.sendGet(new StringBuffer(Const.DDPUNCHDOMAIN).append(Const.ANCHOREVENT)
                            .append("&par_d=").append(date).append(anchor.toString()).toString());
//                    anchorObjs.add(anchor);
                }
            }
            page.setSkip(true);
        } catch (Exception e) {
            failedUrl.append(curUrl + ";  ");
            logger.info("process exception,url:{}" + curUrl);
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.DOUYUEXIT, "url: " + curUrl);
                System.exit(1);
            }
        }
    }

    private String getLastStartTime(long broadcastBegin) {
        return format.format(broadcastBegin);
    }

    @Override
    public Site getSite() {
        return this.site;
    }

    public static void crawler(String[] args) {
        job = args[0];//longzhuanchor
        date = args[1];//20161114
        hour = args[2];//10
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        long start = System.currentTimeMillis();
        Spider.create(new LongzhuAnchorProcessor()).thread(6).addUrl(firUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        long end = System.currentTimeMillis();
        long secs = (end - start) / 1000;
        logger.info(job + ",用时:" + end + "-" + start + "=" + secs + "秒," + "请求数:" + requests + ",qps:" + (requests / secs));
//        for (Anchor anchor : anchorObjs) {
//            anchors.add(anchor.toString());
//        }
//        CommonTools.writeAndMail(hivePaht, Const.LONGZHUFINISH, anchors);
    }
}
