package com.pandatv.processor;

import com.jayway.jsonpath.JsonPath;
import com.pandatv.common.Const;
import com.pandatv.common.PandaProcessor;
import com.pandatv.downloader.credentials.PandaDownloader;
import com.pandatv.pojo.Anchor;
import com.pandatv.tools.CommonTools;
import com.pandatv.tools.MailTools;
import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.ConsolePipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/11.
 */
public class HuyaAnchorProcessor extends PandaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuyaAnchorProcessor.class);
//    private static String url = "http://www.huya.com/cache.php?m=Live&do=ajaxAllLiveByPage&pageNum=1&page=";
    private static String url = "http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page=";
    private static int exCnt;
    @Override
    public void process(Page page) {
        String url = page.getUrl().toString();
        try {
            logger.info("process url:{}", url);
            List<String> all = page.getJson().jsonPath("$.data.datas").all();
            if (all.size() > 0) {
                String newUrl = this.url + (Integer.parseInt(url.substring(url.lastIndexOf('=') + 1)) + 1);
                page.addTargetRequest(newUrl);
                JSONArray list = JsonPath.read(page.getJson().get(), "$.data.datas");
                for (int i = 0; i < list.size(); i++) {
                    String jsonStr = list.get(i).toString();
                    String rid = JsonPath.read(jsonStr, "$.privateHost");
                    String name = JsonPath.read(jsonStr, "$.nick");
                    String title = JsonPath.read(jsonStr, "$.introduction");
                    String category = JsonPath.read(jsonStr, "$.gameFullName");
                    String popularityStr = JsonPath.read(jsonStr, "$.totalCount");
                    int popularityNum = Integer.parseInt(popularityStr);
                    Anchor anchor = new Anchor();
                    anchor.setRid(rid);
                    anchor.setName(name);
                    anchor.setTitle(title);
                    anchor.setCategory(category);
                    anchor.setPopularityStr(popularityStr);
                    anchor.setPopularityNum(popularityNum);
                    anchor.setJob(job);
                    anchor.setPlat(Const.HUYA);
                    anchor.setGame(Const.GAMEALL);
                    anchor.setUrl(url);
                    anchorObjs.add(anchor);
                }
            }
        }catch (Exception e){
            failedUrl.append(url + ";  ");
            logger.info("process exception,url:{},html:{}" + url, page.getHtml());
            e.printStackTrace();
            if (exCnt++ > Const.EXTOTAL) {
                MailTools.sendAlarmmail(Const.DOUYUEXIT, "url: " + url);
                System.exit(1);
            }
        }

    }

    @Override
    public Site getSite() {
        return this.site.setSleepTime(1);
    }

    public static void crawler(String[] args) {
        job = args[0];//huyaanchor
        date = args[1];
        hour = args[2];
        if (args.length == 4 && args[3].contains(",")) {
            mailHours = args[3];
        }
        String hivePaht = Const.COMPETITORDIR + "crawler_anchor/" + date;
        String firstUrl = "http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page=1";
        Spider.create(new HuyaAnchorProcessor()).thread(1).addUrl(firstUrl).addPipeline(new ConsolePipeline()).setDownloader(new PandaDownloader()).run();
        for (Anchor anchor : anchorObjs) {
            anchors.add(anchor.toString());
        }
        CommonTools.writeAndMail(hivePaht, Const.HUYAFINISH, anchors);
    }

    public static void main(String[] args) {
        args = new String[]{"huyaanchor", "20161111", "16"};
        crawler(args);
    }
}
